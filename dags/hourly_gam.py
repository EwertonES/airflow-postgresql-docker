from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from google.oauth2 import service_account

from datetime import datetime, timedelta
from googleads import ad_manager, errors
import numpy as np
import os
import pandas as pd
from pathlib import Path
import pickle


# Retrieve data from Google Ad Manager API.
def get_gam_api_data(**context):
    # Retrieve context.
    today = datetime.strptime(context['current_date'], "%Y-%m-%d").date()

    creds = service_account.Credentials.from_service_account_file('/opt/airflow/keys/gcp-bg.json')

    # Create report job.
    report_job = {
        'reportQuery': {
            'dimensions': ['DATE', 'CUSTOM_CRITERIA'],
            'columns': ['TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE',
                        'TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM'],
            'dateRangeType': 'CUSTOM_DATE',
            'startDate': today,
            'endDate': today,
        }
    }

    # Initialize a DataDownloader.
    client = ad_manager.AdManagerClient.LoadFromStorage('/opt/airflow/keys/googleads.yaml')
    report_downloader = client.GetDataDownloader(version='v202011')

    try:
    # Run the report and wait for it to finish.
        report_job_id = report_downloader.WaitForReport(report_job)
    except errors.AdManagerReportError as e:
        print('Failed to generate report. Error was: %s' % e)

    # Set export format.
    export_format = 'CSV_DUMP'
    report_file = f'/opt/airflow/dags/bg_gam_{today}_hourly.csv'

    # Download report data.
    report_downloader.DownloadReportToFile(report_job_id, export_format, open(report_file, 'wb'), use_gzip_compression=False)

    # Read report data.
    gam_df = pd.read_csv(report_file, header=0, encoding='utf-8', sep=',')

    # Change data types for currencies.
    for col in ['Column.TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE', 'Column.TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM']:
        gam_df[col] = gam_df[col].astype('float')
        gam_df[col] = gam_df[col].apply(lambda x: x*0.000001).round(2)

    # Change ID format.
    gam_df['Dimension.CUSTOM_CRITERIA'] = gam_df['Dimension.CUSTOM_CRITERIA'].astype('str')
    
    # Rename columns.
    gam_df.rename(columns={'Dimension.DATE': 'DATE',
                           'Dimension.CUSTOM_TARGETING_VALUE_ID': 'CUSTOM_TARGETING_VALUE_ID',
                            'Dimension.CUSTOM_CRITERIA': 'CUSTOM_CRITERIA',
                            'Column.TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE': 'TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE',
                            'Column.TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM': 'TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM'},
                  inplace=True)

    # Send Dataframe to BigQuery
    gam_df.to_gbq(destination_table='gam.gam-hourly', project_id='gcp-bg', if_exists='replace', credentials=creds, table_schema=[
      {'name': 'DATE', 'type': 'DATE'},
      {'name': 'CUSTOM_TARGETING_VALUE_ID', 'type': 'STRING'},
      {'name': 'CUSTOM_CRITERIA', 'type': 'STRING'},
      {'name': 'TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE', 'type': 'FLOAT64'},
      {'name': 'TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM', 'type': 'FLOAT64'},
      ])

    # Remove temp file
    if os.path.exists(report_file):
        os.remove(report_file)
    else:
        print(f"{report_file} não existe.")

# Airflow default args
default_args = {
    'owner': 'Ewerton',
    'depends_on_past': False,
    'email': ['ewerton.souza@begrowth.co'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    }

# Define gam_dag args
hourly_gam_dag = DAG(
    dag_id='hourly_gam_dag',
    description='DAG to retrieve hourly reports from Google Ad Manager.',
    default_args=default_args,
    start_date = datetime(2021, 2, 3),
    schedule_interval = "00 * * * *",   # Set Schedule: Run pipeline once an hour.
    catchup = False,
    )

# Create params.
params = {
    'current_date': '{{ ds }}' # Airflow macro: get execution date (catchup)
}

# Define worker.
worker_google = PythonOperator(
    task_id = 'worker_google',
    python_callable = get_gam_api_data,
    op_kwargs = params,
    dag = hourly_gam_dag,
    )
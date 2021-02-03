from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from google.oauth2 import service_account

from datetime import datetime, timedelta
import json
import numpy as np
import pandas as pd
import requests


# Retrieve data from Facebook API.
def get_fb_api_data(**context):
    # Retrieve context.
    today = context['current_date']
    
    creds = service_account.Credentials.from_service_account_file('/opt/airflow/keys/gcp-bg.json')

    # Facebook tokens and accounts.
    FB_ACCESS_TOKEN = "000000000000"
    FB_ACCOUNT_IDS = ["000000000000"]

    # Fields
    fields = ('account_name',
              'spend', 
              'reach', 
              'impressions', 
              'clicks', 
              )

    # Retrieve data from every account owned.
    fb_accounts = []
    for fb_account_id in FB_ACCOUNT_IDS:
        # Call Facebook Insights API and retrieve data.
        response = requests.get(f'https://graph.facebook.com/v9.0/{fb_account_id}/insights?level=account&time_range={{"since":"{today}","until":"{today}"}}&fields={",".join(fields)}&access_token={FB_ACCESS_TOKEN}')
        fb_data = json.loads(response.text)
        for fb_account in fb_data["data"]:
            fb_accounts.append(fb_account)
    
    # Create dataframe with retrieved data.
    fb_df = pd.DataFrame(fb_accounts)

    # Change currency format.
    for col in ['spend']:
        fb_df[col] = fb_df[col].astype('float').round(2)

    fb_df['source'] = "facebook"

    # Send Dataframe to BigQuery
    fb_df.to_gbq(destination_table='facebook.fb-hourly', project_id='gcp-bg', if_exists='replace', credentials=creds, table_schema=[
      {'name': 'account_name', 'type': 'STRING'},
      {'name': 'spend', 'type': 'FLOAT64'},
      {'name': 'reach', 'type': 'INT64'},
      {'name': 'impressions', 'type': 'INT64'},
      {'name': 'clicks', 'type': 'INT64'},
      {'name': 'date_start', 'type': 'DATE'},
      {'name': 'date_stop', 'type': 'DATE'},
      {'name': 'source', 'type': 'STRING'},
      ])

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

# Define fb_dag args
hourly_fb_dag = DAG(
    dag_id='hourly_fb_dag',
    description='DAG to retrieve hourly reports from Facebook Ads.',
    default_args=default_args,
    start_date = datetime(2021, 2, 3),
    schedule_interval = "00 * * * *",   # Set Schedule: Run pipeline once an hour.
    catchup = False,
    )

# Create params.
params = {
    'current_date': '{{ ds }}', # Airflow macro: get execution date (catchup)
}

# Define worker.
worker_hourly_facebook = PythonOperator(
    task_id = 'worker_hourly_facebook',
    python_callable = get_fb_api_data,
    op_kwargs = params,
    dag = hourly_fb_dag,
    )
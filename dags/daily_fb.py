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
              'campaign_name',
              'adset_name',
              'ad_name', 
              'ad_id', 
              'spend', 
              'reach', 
              'impressions', 
              'clicks', 
              'objective', 
              'conversions',
              'conversion_rate_ranking',
              'engagement_rate_ranking',
              'quality_ranking',
              )

    # Retrieve data from every account owned.
    fb_ads = []
    for fb_account_id in FB_ACCOUNT_IDS:
        # Call Facebook Insights API and retrieve data.
        response = requests.get(f'https://graph.facebook.com/v9.0/{fb_account_id}/insights?level=ad&time_range={{"since":"{today}","until":"{today}"}}&time_increment=1&fields={",".join(fields)}&access_token={FB_ACCESS_TOKEN}')
        fb_data = json.loads(response.text)
        for fb_ad in fb_data["data"]:
            fb_ads.append(fb_ad)
    
    # Create dataframe with retrieved data.
    fb_df = pd.DataFrame(fb_ads)

    # Select custom conversion to use as lead.
    for row in range(len(fb_df.index)):
        lead_cell = fb_df.loc[row, 'conversions']
        try:
            for lead_value in lead_cell:
                if lead_value['action_type'] == "offsite_conversion.fb_pixel_custom.ViewCartao":
                    fb_df.loc[row, 'conversions'] = lead_value['value']
            if lead_cell == fb_df.loc[row, 'conversions']:
                print(f"ERRO - ViewCartao não configurado - {fb_df.loc[row, 'account_name']} -> {fb_df.loc[row, 'campaign_name']} -> {fb_df.loc[row, 'adset_name']} -> {fb_df.loc[row, 'ad_name']}")
                fb_df.loc[row, 'conversions'] = 0
        except TypeError:
            print(f"ERRO - Sem conversões personalizadas - {fb_df.loc[row, 'account_name']} -> {fb_df.loc[row, 'campaign_name']} -> {fb_df.loc[row, 'adset_name']} -> {fb_df.loc[row, 'ad_name']}")
            fb_df.loc[row, 'conversions'] = 0
    
    # Change integer format.
    for col in ['reach', 'impressions', 'clicks', 'conversions']:
        fb_df[col] = fb_df[col].astype('float').round(0)

    # Change currency format.
    for col in ['spend']:
        fb_df[col] = fb_df[col].astype('float').round(2)

    # Change ID format.
    fb_df['ad_id'] = fb_df['ad_id'].astype('str')

    # Send Dataframe to BigQuery
    fb_df.to_gbq(destination_table='facebook.fb-daily', project_id='gcp-bg', if_exists='append', credentials=creds, table_schema=[
      {'name': 'account_name', 'type': 'STRING'},
      {'name': 'campaign_name', 'type': 'STRING'},
      {'name': 'adset_name', 'type': 'STRING'},
      {'name': 'ad_name', 'type': 'STRING'},
      {'name': 'ad_id', 'type': 'STRING'},
      {'name': 'spend', 'type': 'FLOAT64'},
      {'name': 'reach', 'type': 'INT64'},
      {'name': 'impressions', 'type': 'INT64'},
      {'name': 'clicks', 'type': 'INT64'},
      {'name': 'objective', 'type': 'STRING'},
      {'name': 'conversions', 'type': 'INT64'},
      {'name': 'conversion_rate_ranking', 'type': 'STRING'},
      {'name': 'engagement_rate_ranking', 'type': 'STRING'},
      {'name': 'quality_ranking', 'type': 'STRING'},
      {'name': 'date_start', 'type': 'DATE'},
      {'name': 'date_stop', 'type': 'DATE'},
      ])


# Airflow default args
default_args = {
    'owner': 'Ewerton',
    'depends_on_past': False,
    'email': ['ewerton.souza@begrowth.co'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    }

# Define fb_dag args
daily_fb_dag = DAG(
    dag_id='daily_fb_dag',
    description='DAG to retrieve daily reports from Facebook Ads.',
    default_args=default_args,
    start_date = datetime(2021, 2, 1),
    schedule_interval = "00 08 * * *",   # Set Schedule: Run pipeline once a day, at 08:00.
    catchup = True,
    )

# Create params.
params = {
    'current_date': '{{ ds }}', # Airflow macro: get execution date (catchup)
}

# Define worker.
worker_facebook = PythonOperator(
    task_id = 'worker_facebook',
    python_callable = get_fb_api_data,
    op_kwargs = params,
    dag = daily_fb_dag,
    )
def credentials():
    # SET BIGQUERY ENVIRONMENT
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_key_path
    client = bigquery.Client()

    return client
    

def call_amplitude_api(start_date, end_date, API_KEY, SECRET_KEY, COLUMNS_OF_INTEREST, bq_infos, client):
  
  api_main_path = f'https://amplitude.com/api/2/export?start={start_date}&end={end_date}'

  request_result = requests.get(api_main_path, auth = HTTPBasicAuth(API_KEY, SECRET_KEY))

  return generate_events_dict(content = request_result.content, columns_of_interest = COLUMNS_OF_INTEREST, bq_infos = bq_infos, client = client)


def generate_events_dict(content, columns_of_interest, bq_infos, client):
  events_dict = {}
  
  for column in columns_of_interest:
    events_dict[column] = []

  response_files = zipfile.ZipFile(BytesIO(content), 'r')

  for filename in response_files.namelist():
      gz = response_files.read(filename)
      hourly_data = gzip.decompress(gz)

      for serialized_data in hourly_data.decode('utf-8').splitlines():
        event_opened = json.loads(serialized_data)

        for column in columns_of_interest:
          try:
            events_dict[column].append(event_opened[column])
          except:
            events_dict[column].append(None)

  return generate_events_df(events_dict, bq_infos, client)


def generate_events_df(events_dict, bq_infos, client):
  df = pd.DataFrame(events_dict)
  for date_columns in ['client_event_time', 'event_time']:
    df[date_columns] = pd.to_datetime(df[date_columns])

  try:
    df = get_gbq_schema(df = df,
                        project_name = bq_infos['project_id'],
                        dataset = bq_infos['dataset_name'],
                        table_name = bq_infos['table_name'], 
                        columns = df.columns.to_list(), 
                        client = client
                      )
  except:
    create_table_query = f'''
      CREATE TABLE `{bq_infos['project_id']}.{bq_infos['dataset_name']}.{bq_infos['table_name']}`
      (
        amplitude_id INT64,
        app INT64,
        client_event_time TIMESTAMP,
        device_family STRING,
        device_model STRING,
        device_type STRING,
        event_id INT64,
        event_properties STRING,
        event_time TIMESTAMP,
        event_type STRING,
        session_id INT64,
        user_id STRING,
        user_properties STRING
      );
      '''
    query_job = client.query(create_table_query)
    query_job.result()

    df = get_gbq_schema(df = df,
                        project_name = bq_infos['project_id'],
                        dataset = bq_infos['dataset_name'],
                        table_name = bq_infos['table_name'], 
                        columns = df.columns.to_list(), 
                        client = client
                      )
  return df


def get_gbq_schema(df, project_name: str, dataset: set, table_name: str, columns: list, client):
  principal_table = client.get_table(f'{project_name}.{dataset}.{table_name}')
  
  result = ["{0} {1}".format(schema.name,schema.field_type) for schema in principal_table.schema]
  
  schemas = {}
  for item in result:
      schemas[item.split(' ')[0]] = item.split(' ')[1]
  
  for column_name in columns:
      if schemas[column_name] == 'STRING':
        df[column_name] = df[column_name].astype(str)

      elif schemas[column_name] == 'BOOLEAN':
        df[column_name] = df[column_name].astype(bool)

      elif schemas[column_name] == 'TIMESTAMP':
        df[column_name] = pd.to_datetime(df[column_name])

      elif schemas[column_name] == 'NUMERIC':
        df[column_name] = df[column_name].astype(float)

      elif schemas[column_name] == 'INTEGER':
        df[column_name] = df[column_name].astype(int)

  return df


def save_gbq(df, project_id: str, dataset_name: str, table_name: str, if_exists: str):
    pandas_gbq.to_gbq(dataframe = df,
                      destination_table = f'{dataset_name}.temp_{table_name}',
                      project_id = project_id,
                      if_exists = if_exists
                      )


def update_GBQ_table(project_id:str, dataset:str, table:str, client):    
    query = f'''
        DELETE
          `{project_id}.{dataset}.{table}`
        WHERE
          CONCAT(CAST(amplitude_id AS STRING), CAST(event_time AS STRING), event_type) IN (
          SELECT
            CONCAT(CAST(amplitude_id AS STRING), CAST(event_time AS STRING), event_type) AS ID
          FROM
            `{project_id}.{dataset}.temp_{table}`);
        INSERT INTO
          `{project_id}.{dataset}.{table}` (
          SELECT
            *
          FROM
            `{project_id}.{dataset}.temp_{table}`);
        DROP TABLE
          `{project_id}.{dataset}.temp_{table}`
    '''
    query_job = client.query(query)
    query_job.result()

    
def set_conditions_and_start(COLUMNS_OF_INTEREST, API_KEY, SECRET_KEY, BIGQUERY_PROJECT_NAME, BIGQUERY_DATASET_DESTINATION, BIGQUERY_TABLE_DESTINATION):
  client = credentials()

  bq_infos = {'project_id'    : BIGQUERY_PROJECT_NAME,
              'dataset_name'  : BIGQUERY_DATASET_DESTINATION,
              'table_name'    : BIGQUERY_TABLE_DESTINATION,
              'if_exists'     : 'replace'
  } 

  CUSTOM_DATE = None #expected None or 'YYYY-MM-DD'
  if CUSTOM_DATE != None:
    CUSTOM_DATE = datetime.strptime(CUSTOM_DATE, '%Y-%m-%d').date()

  reference_date = CUSTOM_DATE or (date.today() - timedelta(days = 1))
  month_part = '0'+str(reference_date.month) if len(str(reference_date.month)) == 1 else str(reference_date.month)
  day_part = '0'+str(reference_date.day) if len(str(reference_date.day)) == 1 else str(reference_date.day)

  string_date = str(reference_date.year) + month_part + day_part

  start_date = string_date + "T0"
  end_date = string_date + "T23"

  amplitude_df = call_amplitude_api(start_date, end_date, API_KEY, SECRET_KEY, COLUMNS_OF_INTEREST, bq_infos, client)

  save_gbq(df = amplitude_df, 
          project_id = bq_infos['project_id'],
          dataset_name = bq_infos['dataset_name'], 
          table_name = bq_infos['table_name'], 
          if_exists = bq_infos['if_exists'])

  update_GBQ_table(project_id = bq_infos['project_id'], 
                  dataset = bq_infos['dataset_name'],
                  table = bq_infos['table_name'], 
                  client = client)
  
import requests
import gzip
import pandas as pd
import zipfile
import json
import pandas_gbq
import os

from google.cloud import bigquery
from google.oauth2 import service_account
from io import BytesIO
from requests.auth import HTTPBasicAuth
from datetime import date, timedelta, datetime

BIGQUERY_PROJECT_NAME = 'MY_GCP_PROJECT_NAME'
BIGQUERY_DATASET_DESTINATION = 'amplitude'
BIGQUERY_TABLE_DESTINATION = 'events'

API_KEY = "AMPLITUDE_API_KEY_STRING"
SECRET_KEY = "AMPLITUDE_SECRET_KEY_STRING"

COLUMNS_OF_INTEREST = ['amplitude_id', 'app', 'client_event_time', 'device_family', 
                        'device_model', 'device_type', 'event_id', 'event_properties', 
                        'event_time', 'event_type', 'session_id', 'user_id', 'user_properties']

set_conditions_and_start(COLUMNS_OF_INTEREST, API_KEY, SECRET_KEY, BIGQUERY_PROJECT_NAME, BIGQUERY_DATASET_DESTINATION, BIGQUERY_TABLE_DESTINATION)

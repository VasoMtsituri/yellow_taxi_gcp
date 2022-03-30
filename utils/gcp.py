import os
import json
import logging

import pandas as pd
from google.cloud import storage
from sqlalchemy import create_engine
from google.cloud import secretmanager

from constants import GCP_CREDS

SECRET_MANAGER_RESOURCE_ID = 'projects/637440997083/secrets/func_creds/versions/1'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDS
logging.basicConfig(level=logging.DEBUG)


def upload_file_2_bucket(**kwargs):
    ti = kwargs['ti']
    filename = ti.xcom_pull(task_ids='file_checker')
    csv_filename = filename[1]

    storage_client = storage.Client()
    bucket = storage_client.bucket('yellow_taxi_data_nyc')
    blob = bucket.blob(csv_filename)
    blob.upload_from_filename(f'partitions/{csv_filename}')


def access_secret_version():
    """
    Connect to GCP Secret Manager for retrieving postgres credentials

    :return: decoded credentials retrieved from SecretManagerServiceClient as str
    """
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": SECRET_MANAGER_RESOURCE_ID})
    logging.debug('Got response for GET request to SecretManagerServiceClient')

    payload = response.payload.data.decode("UTF-8")
    logging.debug('Response decoded successfully')

    return payload


def connect_2_psql(creds):
    """
    Establish connection to Postgres using creds retrieved from SecretManagerServiceClient

    :param creds: decoded credentials retrieved from SecretManagerServiceClient as str
    :return: sqlalchemy engine for connecting Postgres
    """
    creds = json.loads(creds)

    postgres_creds = creds['postgres_creds']
    db_user = postgres_creds['db_user']
    db_pass = postgres_creds['db_pass']
    db_name = postgres_creds['db_name']
    db_socket_dir = os.environ.get("DB_SOCKET_DIR", "/cloudsql")
    instance_connection_name = postgres_creds['instance_connection_name']

    url = f"postgresql+pg8000://{db_user}:{db_pass}@/{db_name}?unix_sock={db_socket_dir}/{instance_connection_name}/.s.PGSQL.5432"

    logging.debug('Connecting to Postgres')
    engine = create_engine(url)
    logging.debug('Connection to Postgres established successfully')

    return engine


def download_blob(bucket_name, source_blob_name, destination_filename):
    """
    Download file (blob) from the given bucket and store it in /tmp directory

    :param bucket_name: name of bucket in Cloud Storage
    :param source_blob_name: name of file to be downloaded
    :param destination_filename: name of downloaded file
    :return: downloads object located in the given bucket
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    path_csv = f'/tmp/{destination_filename}'

    blob.download_to_filename(path_csv)
    logging.debug(f'{destination_filename} downloaded successfully')


def remove_extension_from_filename(filename):
    name = filename.split('.')[0]
    return name


def main(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    filename = event['name']
    bucket = event['bucket']

    logging.debug('Reading creds...')
    creds = access_secret_version()
    logging.debug(f'Successfully read creds with {len(creds)} values')

    download_blob(bucket, filename, filename)
    csv_path = f'/tmp/{filename}'

    df = pd.read_csv(csv_path)
    df_size = df.shape
    logging.debug(f'Successfully read data with shape {df_size} using Pandas')

    engine = connect_2_psql(creds)

    table_name = remove_extension_from_filename(filename)

    df.to_sql(table_name, con=engine, if_exists='append')
    logging.debug(f'Pandas df with shape {df_size} inserted into the table {table_name} successfully')

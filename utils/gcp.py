import os

import pandas as pd
from google.cloud import storage

from constants import GCP_CREDS

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDS


def upload_file_2_bucket(**kwargs):
    ti = kwargs['ti']
    filename = ti.xcom_pull(task_ids='file_checker')
    csv_filename = filename[1]

    storage_client = storage.Client()
    bucket = storage_client.bucket('yellow_taxi_data_nyc')
    blob = bucket.blob(csv_filename)
    blob.upload_from_filename(f'partitions/{csv_filename}')


def connect_2_psql():

    db_socket_dir = os.environ.get("DB_SOCKET_DIR", "/cloudsql")
    instance_connection_name = 'seismic-hope-345015:europe-west3:yellow-taxi-2015'

    # url = "postgresql+pg8000://{db_user}:{db_pass}@/{db_name}#?unix_sock={db_socket_dir}/{instance_connection_name}/.s.PGSQL.5432"

    # engine = create_engine('postgresql+psycopg2://{db_user}:{db_pass}@{instance_connection_name}/{db_name}')

    # engine = create_engine(url)

    return 'engine'


def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    path_csv = f'/tmp/{destination_file_name}'
    print(f'Path = {path_csv}')

    blob.download_to_filename(path_csv)


def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    download_blob(event['bucket'], event['name'], event['name'])
    csv_path = f"/tmp/{event['name']}"

    df = pd.read_csv(csv_path)

    print('Connecting to Cloud SQL!!!!!!!!!!!!')
    engine = connect_2_psql()
    print('Connected to Cloud SQL successfully!')

    df.to_sql(event['name'], con=engine, if_exists='append')
    print(f'Successfully uploaded to SQL Database')
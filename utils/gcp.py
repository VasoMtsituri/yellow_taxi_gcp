import os
import sys
from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
    "/home/vaso/PycharmProjects/yellow_taxi_gcp/seismic-hope-345015-78d3a80b6173.json"

sys.path.append('../yellow_taxi_gcp')


def upload_file_2_bucket():
    storage_client = storage.Client()

    source_file_name = '/partitions/yellow_taxi_2015_01_04.csv'
    destination_blob_name = 'yellow_taxi_2015_01_04.csv'

    bucket = storage_client.bucket('yellow_taxi_data_nyc')
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name)
    )
from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
    "/home/vaso/PycharmProjects/yellow_taxi_gcp/seismic-hope-345015-78d3a80b6173.json"


def upload():
    storage_client = storage.Client()

    source_file_name = '/home/vaso/PycharmProjects/yellow_taxi_gcp/partitions/yellow_taxi_2015_01_04.csv'
    destination_blob_name = 'yellow_taxi_2015_01_04.csv'

    bucket = storage_client.bucket('yellow_taxi_data_nyc')
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name)
    )

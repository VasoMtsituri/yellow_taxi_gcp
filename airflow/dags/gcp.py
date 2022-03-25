# Imports the Google Cloud client library
from google.cloud import storage
import os

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = "/home/vaso/PycharmProjects/yellow_taxi_gcp/seismic-hope-345015-78d3a80b6173.json"

# Instantiates a client
storage_client = storage.Client()

# The name for the new bucket
bucket_name = "my-new-bucket_jj"

# Creates the new bucket
# bucket = storage_client.create_bucket(bucket_name)

# print("Bucket {} created.".format(bucket.name))

source_file_name = '/home/vaso/PycharmProjects/yellow_taxi_gcp/partitions/yellow_taxi_2015_01_04.csv'
destination_blob_name = 'yellow_taxi_2015_01_04.csv'

bucket = storage_client.bucket(bucket_name)

blob = bucket.blob(destination_blob_name)

blob.upload_from_filename(source_file_name)

print(
    "File {} uploaded to {}.".format(
        source_file_name, destination_blob_name)
)

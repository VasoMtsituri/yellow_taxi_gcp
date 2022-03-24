# Imports the Google Cloud client library
from google.cloud import storage
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/vaso/PycharmProjects/yellow_taxi_gcp/seismic-hope-345015-78d3a80b6173.json"

# Instantiates a client
storage_client = storage.Client()

# The name for the new bucket
bucket_name = "my-new-bucket_jj"

# Creates the new bucket
bucket = storage_client.create_bucket(bucket_name)

print("Bucket {} created.".format(bucket.name))

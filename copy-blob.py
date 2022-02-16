import os
from azure.storage.blob import BlobServiceClient, BlobClient, ResourceTypes, AccountSasPermissions, generate_account_sas
from datetime import datetime, timedelta
source_key = 'CW0IXf2zu9g83iL0hy5gA9nHxznYL57dGLCsbId50kSvkd+DQpFZno8UHGHPj4VVeEH77ySut1me+AStNrNuXA=='
des_key = 'ehuiNs4ukyjroYZWQTjrytJRJFpNBuuk9AHKI2kisc/Vhqw2XD5AUoL+Rt8x8UVBzia2CBMEf1d7GbEWtRUc5g=='
source_account_name = 'storageroza1'
des_account_name = 'storageroza2'
# genearte account sas token for source account
sas_token = generate_account_sas(account_name=source_account_name, account_key=source_key,
                                 resource_types=ResourceTypes(
                                     service=True, container=True, object=True),
                                 permission=AccountSasPermissions(read=True),
                                 expiry=datetime.utcnow() + timedelta(hours=1))
source_blob_service_client = BlobServiceClient(
    account_url=f'https://{source_account_name}.blob.core.windows.net/', credential=source_key)
des_blob_service_client = BlobServiceClient(
    account_url=f'https://{des_account_name}.blob.core.windows.net/', credential=des_key)

source_container_client = source_blob_service_client.get_container_client(
    'arm-container')

#source_blob = source_container_client.get_blob_client('0036b81e-4efb-4c91-9997-ab6c5e52fb1f.txt')
#source_url = source_blob.url+'?'+sas_token

connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING1')

# Create a unique name for the container
target_container_name = "arm-container1"

# Create the BlobServiceClient object which will be used to create a container client
#target_blob_service_client = BlobServiceClient.from_connection_string(connect_str)

#Create the container
#target_container_client = target_blob_service_client.create_container(target_container_name)

def copy_all_blobs():
    blob_list = source_container_client.list_blobs()
    for blob in blob_list:
        source_blob = source_container_client.get_blob_client(blob.name)
        source_url = source_blob.url+'?'+sas_token
        copy_blob(source_blob, source_url)

# copy one blob at a time
def copy_blob(source_blob, source_url):
    des_blob_service_client.get_blob_client(
        target_container_name, source_blob.blob_name).start_copy_from_url(source_url)

copy_all_blobs()
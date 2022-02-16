import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

try:
    print("Azure Blob Storage v" + __version__ + " - Python quickstart sample")

    # Quick start code goes here
    # Retrieve the connection string for use with the application. The storage
    # connection string is stored in an environment variable on the machine
    # running the application called AZURE_STORAGE_CONNECTION_STRING. 
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Create a unique name for the container
    container_name = str(uuid.uuid4())

    #Create the container
    container_client = blob_service_client.create_container(container_name)

    # Create a local directory to hold blob data
    local_path = "./data1"
    os.mkdir(local_path)

    # Create a file in the local data directory to upload and download
    local_file_name = str(uuid.uuid4()) + ".txt" 
    #local_file_name = str(uuid.uuid4()) + ".txt"
    upload_file_path = os.path.join(local_path, local_file_name)

    # Write text to the file
   # for file in upload_file_path:
    with open(upload_file_path, 'w') as file:
        file.write("Blobs are created!")

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
    
   # for file_name in local_files_name:
    #    print("\nUploading to Azure Storage as blob:\n\t" + file_name)

    # Upload the created file
    
    #for file_path in upload_file_path:
    with open(upload_file_path, "rb") as data:
        blob_client.upload_blob(data)
           # blob_index += 1

    print("\nListing blobs...")

    # List the blobs in the container
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        print("\t" + blob.name)

    #Copy blob to another storage
    account_name = os.environ.get('storageroza1')
    # Source
    source_blob = (f"https://{account_name}.blob.core.windows.net/{container_name}/{local_file_name}")

    # Target
    target_container_name = str(uuid.uuid4())

    #Create the container
    target_container_client = blob_service_client.create_container(target_container_name)

    target_file_path = local_file_name
    copied_blob = blob_service_client.get_blob_client(target_container_name, target_file_path)
    copied_blob.start_copy_from_url(source_blob)

    # Clean up
    print("\nPress the Enter key to begin clean up")
    input()

    print("Deleting blob container...")
    container_client.delete_container()

    print("Deleting the local source and downloaded files...")
    os.remove(upload_file_path)
    #os.remove(download_file_path)
    os.rmdir(local_path)

    print("Done")


except Exception as ex:
    print('Exception:')
    print(ex)
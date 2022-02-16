import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings, ContainerClient
#from azure.storage.blob import ContentSettings, ContainerClient
 
# IMPORTANT: Replace connection string with your storage account connection string
# Usually starts with DefaultEndpointsProtocol=https;...
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

 # Create the BlobServiceClient object which will be used to create a container client
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# Container name to be created with
container_name = "arm-container"

#Create the container
#container_client = blob_service_client.create_container(container_name)

 
# Local folder which contains the text files for upload
local_path = "./data"
#os.mkdir(local_path)
 
class AzureBlobFileUploader:
  def __init__(self):
    print("Intializing AzureBlobFileUploader")
 
    # Initialize the connection to Azure storage account
    self.blob_service_client =  BlobServiceClient.from_connection_string(connect_str)

    #Create a container
    self.client_container = blob_service_client.create_container(container_name)

  def create_and_upload_all_files_in_folder(self):
    # Create a 1000 files in the local data directory to upload 
    local_files_name = [str(uuid.uuid4()) + ".txt" for item in range(1000)]
    
    upload_files_path = [os.path.join(local_path, file) for file in local_files_name]

    # Write text to the file
    for file in upload_files_path:
        with open(file, 'w') as file:
            file.write("Blobs are created!")

    # Get all files with txt extension and exclude directories
    all_file_names = [f for f in os.listdir(local_path)
                    if os.path.isfile(os.path.join(local_path, f)) and ".txt" in f]
 
    # Upload each file
    for file_name in all_file_names:
      self.upload_text(file_name)
 
    self.clean_up(upload_files_path)
  def upload_text(self,file_name):
    # Create blob with same name as local file name
    blob_client = self.blob_service_client.get_blob_client(container=container_name,
                                                          blob=file_name)
    # Get full path to the file
    upload_file_path = os.path.join(local_path, file_name)
 
    # Create blob on storage
    # Overwrite if it already exists!
    file_content_setting = ContentSettings(content_type='txt')
    print(f"uploading file - {file_name}")
    with open(upload_file_path, "rb") as data:
      blob_client.upload_blob(data,overwrite=True,content_settings=file_content_setting)

  def clean_up(self, upload_files_path):
    # Clean up
    print("\nPress the Enter key to begin clean up")
    input()

    print("Deleting blob container...")
    self.client_container.delete_container()

    print("Deleting the local source and downloaded files...")
    for file_path in upload_files_path:
      os.remove(file_path)
    #os.remove(download_file_path)
    os.rmdir(local_path)

    print("Done") 
 
# Initialize class and upload files
azure_blob_file_uploader = AzureBlobFileUploader()
azure_blob_file_uploader.create_and_upload_all_files_in_folder()

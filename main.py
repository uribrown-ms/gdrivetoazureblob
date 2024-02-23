import json
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from azure.storage.blob import BlobClient
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import io
import os
from google.oauth2.service_account import Credentials

# Load environment variables from .env file
load_dotenv(dotenv_path='./stuff.env')

# Get the Azure Storage container name
container_name = os.getenv('AZURE_STORAGE_CONTAINER_NAME')

# Google Drive setup
creds_path = os.getenv('GOOGLE_DRIVE_CREDENTIALS_PATH')  # Get path to credentials JSON file from environment variable

# Check if creds_path is None
if creds_path is None:
    raise Exception("Failed to load GOOGLE_DRIVE_CREDENTIALS_PATH environment variable. Please check your .env file.")

# If we've made it this far, we can try to open the file
with open(creds_path, 'r') as f:
    creds_json = json.load(f)

creds = Credentials.from_service_account_info(creds_json)

drive_service = build('drive', 'v3', credentials=creds)

# Azure Blob Storage setup
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

# Get the list of all files in the Google Drive folder
folder_id = os.getenv('GOOGLE_DRIVE_FOLDER_ID')
if folder_id is None:
    raise Exception("Failed to load GOOGLE_DRIVE_FOLDER_ID environment variable. Please check your .env file.")
query = f"'{folder_id}' in parents"
results = drive_service.files().list(q=query).execute()
items = results.get('files', [])
print(f"Number of files to download: {len(items)}")

for item in items:
    print(f"Downloading file {item['name']} from Google Drive...")

    # Define the MIME types for Google Docs and Sheets
    mime_types = {
    'application/vnd.google-apps.document': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.google-apps.presentation': 'application/vnd.openxmlformats-officedocument.presentationml.presentation'
    }

    fh = io.BytesIO()

    # If the file is a Google Doc or Sheet, convert it to Word or Excel
    if item['mimeType'] in mime_types:
        request = drive_service.files().export_media(fileId=item['id'], mimeType=mime_types[item['mimeType']])
    if item['mimeType'] == 'application/vnd.google-apps.document':
        item['name'] += '.docx'
    elif item['mimeType'] == 'application/vnd.google-apps.spreadsheet':
        item['name'] += '.xlsx'
    elif item['mimeType'] == 'application/vnd.google-apps.presentation':
        item['name'] += '.pptx'
    else:
        request = drive_service.files().get_media(fileId=item['id'])

    # Download the file
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()

    # Upload the file to Azure Blob Storage
    print(f"Uploading file {item['name']} to Azure Blob Storage...")
    blob_client = BlobClient.from_connection_string(connect_str, container_name, item['name'])
    fh.seek(0)  # Reset the file pointer to the beginning
    blob_client.upload_blob(fh.read(), overwrite=True)

    # Check if the blob was uploaded successfully
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    check_blob_client = blob_service_client.get_blob_client(container_name, item['name'])

    if check_blob_client.exists():
        print(f"Successfully uploaded {item['name']} to Azure Blob Storage.")
    else:
        print(f"Failed to upload {item['name']} to Azure Blob Storage.")

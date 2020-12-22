import io
import pickle
import os

from config import KartkaConfig, LayoutConfig, StoreConfig
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.file']


def login_to_drive(config: LayoutConfig):
    """If the user has not registered this app with their Google Drive before, starts the login flow.
       Then builds the Google Drive resource and returns it."""

    creds = None
    pickle_path = os.path.join(config.data_dir, 'token.pickle')
    if os.path.exists(pickle_path):
        with open(pickle_path, 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(config.drive_credentials, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(pickle_path, 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)


def init_drive(config: StoreConfig, drive_client) -> str:
    """Initialises the Google Drive with the configured `drive_kartka_dir`, creating it if necessary."""

    response = drive_client.files().list(
        q=f"name = '{config.drive_kartka_dir}' and mimeType = 'application/vnd.google-apps.folder'",
        spaces='drive',
        fields='files(id, name)').execute()

    found_files = response.get('files', [])

    if not found_files:
        print(f'No {config.drive_kartka_dir} dir found. Setting up..')
        metadata = {
            'name': config.drive_kartka_dir,
            'mimeType': 'application/vnd.google-apps.folder',
        }
        file = drive_client.files().create(body=metadata, fields='id').execute()
        return file.get('id')
    else:
        return found_files[0].get('id')


def download_file(drive_client, name: str, id: str) -> bytes:
    """Downloads `name` (with file_id `id`) from Google Drive and returns it as `bytes`."""

    print(f'Downloading {name}..')

    request = drive_client.files().get_media(fileId=id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print('Downloaded %d%%..' % int(status.progress() * 100))

    return fh.getvalue()


async def foreach_file(config: KartkaConfig, drive_client, fields, fun):
    """Iterates through all files stored in the `drive_kartka_dir` and applies `fun` to them."""

    page_token = None

    while True:
        response = drive_client.files().list(
            q=f"'{config.drive_base_id}' in parents",
            spaces='drive',
            fields=f'nextPageToken, {fields}',
            pageToken=page_token
        ).execute()

        for file in response.get('files', []):
            await fun(file)

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break


def upload_pdf_file(config: KartkaConfig, drive_client, name: str, path: str) -> str:
    """Uploads the pdf file at `path` to the `drive_kartka_dir` with the name `name`. Returns the file id."""

    file_metadata = {'name': name, 'parents': [config.drive_base_id]}
    media = MediaFileUpload(path, mimetype='application/pdf')
    response = drive_client.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return response.get('id')

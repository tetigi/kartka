#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

from typing import List

from PIL import Image
import configparser
import pytesseract
import asyncio
import tempfile
from asonic import Client
from asonic.enums import Channel
from datetime import datetime
import os.path
import argparse
import argcomplete
import pickle
import sys
from googleapiclient.discovery import build, MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from dataclasses import dataclass


SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.file']


@dataclass
class LayoutConfig:
    data_dir: str
    ingest_dir: str
    drive_credentials: str


@dataclass
class SearchConfig:
    collection_name: str
    bucket_name: str
    sonic_host: str
    sonic_port: int
    sonic_password: str


@dataclass
class StoreConfig:
    drive_kartka_dir: str


@dataclass
class KartkaConfig:
    layout: LayoutConfig
    search: SearchConfig
    store: StoreConfig
    drive_base_id: str = None


async def process(config: KartkaConfig, sonic: Client, drive, files: List[str]):
    """Processes the provided images, generating their containing texts and packaging them into zips"""
    contents = ''
    converted_images = []
    for file in files:
        with Image.open(file) as img:
            print(f'Processing {file}..')
            contents += pytesseract.image_to_string(img)
            converted_images.append(img.convert('RGB'))
        contents += '\n'

    now = datetime.now()
    temp_dir = tempfile.TemporaryDirectory()
    output_file = now.strftime('%Y-%m-%d-%H%M') + '.pdf'
    output_path = os.path.join(temp_dir.name, output_file)

    print('Saving images as pdf..')
    converted_images[0].save(output_path, save_all=True, append_images=converted_images[1:])

    print('Uploading to drive..')
    file_metadata = {'name': output_file, 'parents': [config.drive_base_id]}
    media = MediaFileUpload(output_path, mimetype='application/pdf')
    response = drive.files().create(body=file_metadata, media_body=media, fields='id').execute()

    print('Ingesting to sonic..')
    encoded_id = encode_id(now, response.get('id'))
    print(encoded_id)
    for line in contents.splitlines():
        if line and not line.isspace():
            await sonic.push(config.search.collection_name, config.search.bucket_name, encoded_id, line)


def encode_id(dt: datetime, file_id: str) -> str:
    return f'{dt.strftime("%Y-%m-%d_%H%M")}~{file_id}'


def decode_id(encoded_id: str) -> (str, str):
    dt_str, file_id = encoded_id.split('~')
    return dt_str, file_id


async def ingest_cmd(config: KartkaConfig, drive, args):
    c = create_sonic_client(config)
    await c.channel(Channel.INGEST)
    await process(config, c, drive, args.files)
    print('Done')


async def search_cmd(config: KartkaConfig, _, args):
    c = create_sonic_client(config)
    await c.channel(Channel.SEARCH)

    entries = await c.query(
        config.search.collection_name,
        config.search.bucket_name,
        ' '.join(args.search_terms))

    sorted_entries = reversed(sorted((decode_id(entry.decode('utf-8')) for entry in entries), key=lambda pair: pair[0]))
    for (date_str, file_id) in sorted_entries:
        print(f'{date_str.replace("_", " ")}\t -> https://drive.google.com/file/d/{file_id}/view?usp=sharing')


async def check_cmd(config: KartkaConfig, drive, args):
    c = create_sonic_client(config)
    await c.channel(Channel.SEARCH)

    print(await c.ping())


def create_sonic_client(config: KartkaConfig) -> Client:
    return Client(host=config.search.sonic_host,
                  port=config.search.sonic_port,
                  password=config.search.sonic_password)


def login_to_drive(config: LayoutConfig):
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


def init_drive(drive) -> str:
    response = drive.files().list(
        q="name = 'kartka' and mimeType = 'application/vnd.google-apps.folder'",
        spaces='drive',
        fields='files(id, name)').execute()

    got = response.get('files', [])

    if not got:
        print('Setting up drive directory..')
        metadata = {
            'name': 'kartka',
            'mimeType': 'application/vnd.google-apps.folder',
        }
        file = drive.files().create(body=metadata, fields='id').execute()
        print(f'Folder ID: {file.get("id")}')
        return file.get('id')
    else:
        return got[0].get('id')


def read_section(config, key):
    if config.has_section(key):
        return config[key]
    else:
        print(f'No section {key} found in provided configuration')
        sys.exit(1)


def read_conf(section, key):
    if key in section:
        return section[key]
    else:
        print(f'No {key} found in provided section')
        sys.exit(1)


def get_config(location) -> KartkaConfig:
    config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
    config.read(location)

    layout = read_section(config, 'layout')
    search = read_section(config, 'search')
    store = read_section(config, 'store')

    return KartkaConfig(
        layout=LayoutConfig(
            data_dir=read_conf(layout, 'data_dir'),
            ingest_dir=read_conf(layout, 'ingest_dir'),
            drive_credentials=read_conf(layout, 'drive_credentials'),
        ),
        search=SearchConfig(
            collection_name=read_conf(search, 'collection_name'),
            bucket_name=read_conf(search, 'bucket_name'),
            sonic_host=read_conf(search, 'sonic_host'),
            sonic_port=read_conf(search, 'sonic_port'),
            sonic_password=read_conf(search, 'sonic_password'),
        ),
        store=StoreConfig(
            drive_kartka_dir=read_conf(store, 'drive_kartka_dir'),
        ),
    )


def init_dirs(config: KartkaConfig):
    os.makedirs(os.path.join(config.layout.data_dir, 'sonic'), exist_ok=True)
    os.makedirs(os.path.join(config.layout.data_dir, 'files'), exist_ok=True)

    os.makedirs(config.layout.ingest_dir, exist_ok=True)


def main(args):
    config = get_config(args.config)
    init_dirs(config)

    drive_service = login_to_drive(config.layout)
    config.drive_base_id = init_drive(drive_service)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(arguments.func(config, drive_service, arguments))


def sonic_suggestions(prefix, parsed_args, **kwargs):
    if len(prefix) < 2:
        return []
    else:
        async def do_it():
            config = get_config(parsed_args.config)
            sonic = create_sonic_client(config)
            await sonic.channel(Channel.SEARCH)
            suggestions = await sonic.suggest(config.search.collection_name, config.search.bucket_name, prefix)
            return list(s.decode('utf-8') for s in suggestions)

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(do_it())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kartka')
    parser.add_argument('--config', required=False, help='Kartka configuration file location', default='kartka.cfg')

    subparsers = parser.add_subparsers(dest='mode', help='The mode to use')
    subparsers.required = True

    check_parser = subparsers.add_parser('check', help='check connections')
    check_parser.set_defaults(func=check_cmd)

    ingest_parser = subparsers.add_parser('ingest', help='ingest a letter')
    ingest_parser.add_argument('files', nargs='+', help='in-order files to ingest')
    ingest_parser.set_defaults(func=ingest_cmd)

    search_parser = subparsers.add_parser('search', help='search for letters')
    search_parser.add_argument('search_terms', nargs='+', help='terms to search for').completer = sonic_suggestions
    search_parser.set_defaults(func=search_cmd)

    argcomplete.autocomplete(parser)
    arguments = parser.parse_args()

    main(arguments)

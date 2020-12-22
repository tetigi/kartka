#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK


import drive
from config import KartkaConfig, get_config

from PIL import Image
import pytesseract
import asyncio
import tempfile
from typing import List
from asonic import Client
from asonic.enums import Channel
from datetime import datetime
import os.path
import pathlib
import argparse
import argcomplete
import sys
from dataclasses import dataclass
from pdf2image import convert_from_bytes


@dataclass
class KartkaDocument:
    images: List[any]
    name: str
    contents: str = None
    drive_id: str = None
    created_time: datetime = None


async def ingest_and_upload(config: KartkaConfig, sonic: Client, drive_client, doc: KartkaDocument):
    """Processes the provided images, generating their containing texts and packaging them into pdfs"""
    converted_images = []

    if not doc.contents:
        doc.contents = ''
        for img in doc.images:
            with img:
                doc.contents += pytesseract.image_to_string(img)
                converted_images.append(img.convert('RGB'))
            doc.contents += '\n'

    temp_dir = tempfile.TemporaryDirectory()
    output_path = os.path.join(temp_dir.name, doc.name)

    if not doc.drive_id:
        print('Saving images as pdf..')
        converted_images[0].save(output_path, save_all=True, append_images=converted_images[1:])

        print('Uploading to drive..')
        doc.drive_id = drive.upload_pdf_file(config, drive_client, doc.name, output_path)

    print('Ingesting to sonic..')
    if not doc.created_time:
        doc.created_time = datetime.now()
    encoded_id = encode_id(doc.created_time, doc.drive_id)
    for line in doc.contents.splitlines():
        if line and not line.isspace():
            await sonic.push(config.search.collection_name, config.search.bucket_name, encoded_id, line)


def encode_id(dt: datetime, file_id: str) -> str:
    return f'{dt.strftime("%Y-%m-%d_%H%M")}~{file_id}'


def decode_id(encoded_id: str) -> (str, str):
    dt_str, file_id = encoded_id.split('~')
    return dt_str, file_id


async def ingest_cmd(config: KartkaConfig, drive_client, args):
    c = create_sonic_client(config)
    await c.channel(Channel.INGEST)

    now = datetime.now()
    output_file = now.strftime('%Y-%m-%d-%H%M') + '.pdf'
    doc = KartkaDocument(
        images=list(Image.open(f) for f in args.files),
        name=output_file,
    )
    await ingest_and_upload(config, c, drive_client, doc)
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


async def check_cmd(config: KartkaConfig, drive_client, _):
    c = create_sonic_client(config)
    await c.channel(Channel.SEARCH)

    assert(await c.ping() == b'PONG')
    drive_client.files().list().execute()
    print('All checks passed.')


async def hydrate_cmd(config: KartkaConfig, drive_client, args):
    c = create_sonic_client(config)
    await c.channel(Channel.INGEST)

    async def fun(drive_file):
        file_bytes = drive.download_file(drive_client, drive_file.get('name'), drive_file.get('id'))

        print('Converting to images..')
        imgs = convert_from_bytes(file_bytes, grayscale=True, thread_count=8, dpi=100)
        doc = KartkaDocument(
            images=imgs,
            name=drive_file.get('name'),
            drive_id=drive_file.get('id'),
            created_time=datetime.strptime(drive_file.get('createdTime'), '%Y-%m-%dT%H:%M:%S.%fZ')
        )
        print(f'Ingesting..')
        await ingest_and_upload(config, c, drive_client, doc)

    print('Starting hydration from drive..')
    await drive.foreach_file(config, drive_client, 'files(id, name, createdTime)', fun)

    print('Hydration complete!')


async def scan_cmd(config: KartkaConfig, drive_client, args):
    scan_dir = config.layout.scan_dir
    print('Starting scan workflow..')
    print('Please begin scanning documents into', config.layout.scan_dir)
    input('When finished, press enter')

    print('Attempting to consume..')
    files_in_dir = [os.path.join(scan_dir, path)
                    for path in os.listdir(scan_dir)
                    if os.path.isfile(os.path.join(scan_dir, path))]
    files_in_dir.sort(key=lambda p: pathlib.Path(p).stat().st_ctime)
    print('Will ingest these files in this order:')
    for i, f in enumerate(files_in_dir):
        print(f'{i + 1}) {f}')

    result = input('Start ingesting? Y/n').lower()
    if result == 'y' or result == '':
        args.files = files_in_dir
        await ingest_cmd(config, drive, args)
    else:
        sys.exit(0)

    result = input('Delete scanned files? Y/n').lower()
    if result == 'y' or result == '':
        for f in files_in_dir:
            print(f'Removing file {f}..')
            os.remove(f)
    print('Scan workflow complete!')


def create_sonic_client(config: KartkaConfig) -> Client:
    return Client(host=config.search.sonic_host,
                  port=config.search.sonic_port,
                  password=config.search.sonic_password)


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


def init_dirs(config: KartkaConfig):
    os.makedirs(os.path.join(config.layout.data_dir, 'sonic'), exist_ok=True)
    os.makedirs(os.path.join(config.layout.data_dir, 'files'), exist_ok=True)

    os.makedirs(config.layout.scan_dir, exist_ok=True)


def main(args):
    config = get_config(args.config)
    init_dirs(config)

    drive_service = drive.login_to_drive(config.layout)
    config.drive_base_id = drive.init_drive(config.store, drive_service)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(arguments.func(config, drive_service, arguments))


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

    scan_parser = subparsers.add_parser('scan', help='start a scan ingest letter')
    scan_parser.set_defaults(func=scan_cmd)

    search_parser = subparsers.add_parser('search', help='search for letters')
    search_parser.add_argument('search_terms', nargs='+', help='terms to search for').completer = sonic_suggestions
    search_parser.set_defaults(func=search_cmd)

    hydrate_parser = subparsers.add_parser('hydrate', help='hydrate sonic from drive')
    hydrate_parser.set_defaults(func=hydrate_cmd)

    argcomplete.autocomplete(parser)
    arguments = parser.parse_args()

    main(arguments)

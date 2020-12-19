#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

from typing import List

from PIL import Image
import pytesseract
import asyncio
from asonic import Client
from asonic.enums import Channel
from zipfile import ZipFile
from datetime import datetime
import os.path
import argparse


COLLECTION = 'letters'
BUCKET = 'default'
ROOT = '/Users/deepthought/Desktop'


async def process(sonic: Client, output_dir: str, files: List[str]):
    """Processes the provided images, generating their containing texts and packaging them into zips"""
    contents = ''
    for file in files:
        with Image.open(file) as img:
            print(f'Processing {file}..')
            contents += pytesseract.image_to_string(img)
        contents += '\n'

    now = datetime.now()
    output_file = now.strftime('%Y-%m-%d-%H%M') + '.zip'
    output_path = output_dir + '/' + output_file

    print('Zipping up images..')
    with ZipFile(output_path, 'w') as zip:
        for i, file in enumerate(files):
            zip.write(file, f'page_{i}{os.path.splitext(file)[1]}')

    print('Ingesting to sonic..')
    for line in contents.splitlines():
        if line and not line.isspace():
            await sonic.push(COLLECTION, BUCKET, output_file, line)


async def ingest(args):
    c = Client(host='127.0.0.1', port=1491, password='SecretPassword')
    await c.channel(Channel.INGEST)
    await process(c, ROOT, args.files)


async def search(args):
    c = Client(host='127.0.0.1', port=1491, password='SecretPassword')
    await c.channel(Channel.SEARCH)
    entries = await c.query(COLLECTION, BUCKET, ' '.join(args.search_terms))
    for entry in entries:
        print(f'{ROOT}/{entry.decode("utf-8")}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kartka')
    subparsers = parser.add_subparsers(dest='mode', help='The mode to use')
    subparsers.required = True

    ingest_parser = subparsers.add_parser('ingest', help='ingest a letter')
    ingest_parser.add_argument('files', nargs='+', help='in-order files to ingest')
    ingest_parser.set_defaults(func=ingest)

    search_parser = subparsers.add_parser('search', help='search for letters')
    search_parser.add_argument('search_terms', nargs='+', help='terms to search for')
    search_parser.set_defaults(func=search)

    arguments = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(arguments.func(arguments))


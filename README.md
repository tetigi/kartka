# Kartka #

Kartka is a tool for people who hate paper. It provides a workflow for scanning, indexing, and then uploading letters
you receive for later retrieval.

Kartka uses (your own) Google Drive for storage, and Sonic for efficient and simple indexing.

## How it works ##

```buildoutcfg
Kartka

positional arguments:
  {check,ingest,scan,search,hydrate}
                        The mode to use
    check               check connections
    ingest              ingest a letter
    scan                start a scan 
    search              search for letters
    hydrate             hydrate sonic from drive

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG       Kartka configuration file location
```

`./kartka.py ingest scan_page_1.png scan_page_2.png`

```text
> ./kartka.py search mortgage

2020-12-22 1512  -> https://drive.google.com/file/d/<id>/view?usp=sharing
2020-11-18 1048  -> https://drive.google.com/file/d/<id>/view?usp=sharing
2020-11-12 1142  -> https://drive.google.com/file/d/<id>/view?usp=sharing
```

There are 3 basic commands:

`ingest`: Ingest one or more files from the command line, index them, and then upload them.

`scan`: Begin a 'scan' workflow where you can scan your pages into a configured directory, and then ingest once finished.

`search`: Search for letters that contain all of the provided keywords. Supports search suggestions via autocomplete.

## Installation ##

To use `kartka`, you will need the following:

- A functioning Sonic instance: https://github.com/valeriansaliou/sonic
- Tesseract OCR: https://github.com/madmaze/pytesseract#installation
- Install the python `requirements.txt`, via a venv or otherwise: 
  https://pip.pypa.io/en/stable/user_guide/#requirements-files
  
- A drive-enabled `credentials.json`: I use https://developers.google.com/drive/api/v3/quickstart/python because I'm lazy.

Then copy `kartka.cfg.example` to `kartka.cfg` and configure it with your preferred settings. I personally like to run
my Sonic instance on a Raspberry Pi (for convenience), but it can be ran locally as well.

Move the `credentials.json` to your `data_dir` (configured in the `cfg` above).

Now you're ready to go! On first run `kartka` will run the auth-flow on your new drive-app. After this, it will re-use
the stored token.


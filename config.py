import configparser
import sys

from dataclasses import dataclass


@dataclass
class LayoutConfig:
    data_dir: str
    scan_dir: str
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


def _read_section(config, key):
    if config.has_section(key):
        return config[key]
    else:
        print(f'No section {key} found in provided configuration')
        sys.exit(1)


def _read_conf(section, key):
    if key in section:
        return section[key]
    else:
        print(f'No {key} found in provided configuration')
        sys.exit(1)


def get_config(location) -> KartkaConfig:
    """Attempts to load the KartkaConfig from the provided location"""

    config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
    config.read(location)

    layout = _read_section(config, 'layout')
    search = _read_section(config, 'search')
    store = _read_section(config, 'store')

    return KartkaConfig(
        layout=LayoutConfig(
            data_dir=_read_conf(layout, 'data_dir'),
            scan_dir=_read_conf(layout, 'scan_dir'),
            drive_credentials=_read_conf(layout, 'drive_credentials'),
        ),
        search=SearchConfig(
            collection_name=_read_conf(search, 'collection_name'),
            bucket_name=_read_conf(search, 'bucket_name'),
            sonic_host=_read_conf(search, 'sonic_host'),
            sonic_port=_read_conf(search, 'sonic_port'),
            sonic_password=_read_conf(search, 'sonic_password'),
        ),
        store=StoreConfig(
            drive_kartka_dir=_read_conf(store, 'drive_kartka_dir'),
        ),
    )



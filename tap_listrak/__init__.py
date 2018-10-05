#!/usr/bin/env python3

import json
import sys

import singer
from singer import metadata

from tap_listrak.client import ListrakClient
from tap_listrak.discover import discover
from tap_listrak.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'start_date',
    'client_id',
    'client_secret'
]

def do_discover():
    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')

@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with ListrakClient(parsed_args.config['client_id'],
                       parsed_args.config['client_secret']) as client:
        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            num_activity_days = int(parsed_args.config.get('num_activity_days', 7))
            sync(client,
                 parsed_args.catalog,
                 parsed_args.state,
                 parsed_args.config['start_date'],
                 num_activity_days)

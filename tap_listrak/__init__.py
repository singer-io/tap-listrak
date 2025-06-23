#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams as streams_
from .context import Context
from . import schemas

REQUIRED_CONFIG_KEYS = ["start_date", "username", "password"]
LOGGER = singer.get_logger()

def check_credentials_are_authorized(ctx):
    pass  # Temporarily disabled due to no credentials

def discover(ctx):
    # Skip credential check
    # check_credentials_are_authorized(ctx)

    catalog = Catalog([])

    for tap_stream_id in schemas.stream_ids:
        schema_dict = schemas.load_schema(tap_stream_id)
        schema = Schema.from_dict(schema_dict)

        mdata = metadata.get_standard_metadata(
            schema_dict,
            key_properties=schemas.PK_FIELDS[tap_stream_id])
        
        mdata = metadata.to_map(mdata)

        # Auto-include critical parent streams
        if tap_stream_id in ['lists', 'messages']:
            mdata = metadata.write(mdata, (), 'inclusion', 'automatic')

        # Mark each field as "selected": true (if not unsupported)
        for field_name in schema_dict['properties'].keys():
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')

        md_list = metadata.to_list(mdata)
        for md in md_list:
            if md["metadata"].get("inclusion") != "unsupported":
                md["metadata"]["selected"] = True

        catalog.streams.append(CatalogEntry(
            stream=tap_stream_id,
            tap_stream_id=tap_stream_id,
            key_properties=schemas.PK_FIELDS[tap_stream_id],
            schema=schema,
            metadata=md_list
        ))

    return catalog

def sync(ctx):
    for tap_stream_id in ctx.selected_stream_ids:
        schemas.load_and_write_schema(tap_stream_id)
    streams_.sync_lists(ctx)
    ctx.write_state()

def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    ctx = Context(args.config, args.state)
    if args.discover:
        discover(ctx).dump()
        print()
    else:
        ctx.catalog = Catalog.from_dict(args.properties) \
            if args.properties else discover(ctx)
        sync(ctx)

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise

if __name__ == "__main__":
    main()

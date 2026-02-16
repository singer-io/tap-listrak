#!/usr/bin/env python3
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams as streams_
from .context import Context
from . import schemas

REQUIRED_CONFIG_KEYS = ["start_date", "username", "password"]
LOGGER = singer.get_logger()

STREAM_DEPENDENCIES = {
    'messages': 'lists',
    'message_bounces': 'messages',
    'message_clicks': 'messages',
    'message_opens': 'messages',
    'message_reads': 'messages',
    'message_sends': 'messages',
    'message_unsubs': 'messages',
    'subscribed_contacts': 'lists'
}


def check_credentials_are_authorized(ctx):
    pass


def discover(ctx):
    check_credentials_are_authorized(ctx)
    catalog = Catalog([])

    for tap_stream_id in schemas.stream_ids:
        schema_dict = schemas.load_schema(tap_stream_id)
        schema = Schema.from_dict(schema_dict)

        mdata = metadata.get_standard_metadata(schema_dict,
                                               key_properties=schemas.PK_FIELDS[tap_stream_id])

        mdata = metadata.to_map(mdata)

        # NB: `lists` and `messages` are required for their substreams.
        # This is an approximation of the initial functionality using
        # metadata, which marked them as `selected=True` in the schema.
        if tap_stream_id in ['lists', 'messages']:
            mdata = metadata.write(mdata, (), 'inclusion', 'automatic')

        for field_name in schema_dict['properties'].keys():
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')

        if parent_stream := STREAM_DEPENDENCIES.get(tap_stream_id):
            mdata = metadata.write(mdata, (), 'parent-tap-stream-id', parent_stream)

        catalog.streams.append(CatalogEntry(
            stream=tap_stream_id,
            tap_stream_id=tap_stream_id,
            key_properties=schemas.PK_FIELDS[tap_stream_id],
            schema=schema,
            metadata = metadata.to_list(mdata)
        ))
    return catalog


def sync(ctx):
    """
    Sync function updated to respect stream dependencies.

    This approach is necessary because:
    1. Child streams depend on parent stream data and cannot be synced independently
    2. Parent streams must be synced first to provide the necessary context and IDs for their child streams
    3. Loading parent stream schemas upfront prevents state management issues and ensures schema consistency across dependent streams
    """

    # All lists-dependent streams are synced through sync_lists
    LOGGER.info("Syncing lists and its dependent streams")

    # Collect all parent streams that need to be loaded
    parent_streams_to_load = set()

    for stream_id in ctx.selected_stream_ids:
        # Add the stream itself
        parent_streams_to_load.add(stream_id)

        # Add all parent streams in the dependency chain
        current_stream = stream_id
        while current_stream in STREAM_DEPENDENCIES:
            parent_stream = STREAM_DEPENDENCIES[current_stream]
            parent_streams_to_load.add(parent_stream)
            current_stream = parent_stream

    # Load schemas for all collected parent streams
    for stream_id in parent_streams_to_load:
        schemas.load_and_write_schema(stream_id)

    streams_.sync_lists(ctx)

    ctx.write_state()


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    ctx = Context(args.config, args.state)
    if args.discover:
        discover(ctx).dump()
    elif args.catalog:
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

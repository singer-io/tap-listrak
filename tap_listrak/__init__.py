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
    
    Instead of calling sync functions dynamically, this function ensures:
    1. Parent streams are validated to be selected if child streams are selected
    2. All syncing goes through the appropriate parent sync functions (sync_lists for lists-dependent streams)
    
    This maintains the original design where:
    - sync_lists handles: lists, messages (and its substreams), subscribed_contacts
    - Individual sync functions only exist for independent streams
    """
    
    # Check if any stream requires lists to be synced
    lists_dependent_streams = {'lists', 'messages', 'subscribed_contacts', 
                               'message_clicks', 'message_opens', 'message_reads',
                               'message_sends', 'message_unsubs', 'message_bounces'}
    
    needs_lists_sync = any(stream in ctx.selected_stream_ids for stream in lists_dependent_streams)
    
    if needs_lists_sync:
        # All lists-dependent streams are synced through sync_lists
        LOGGER.info("Syncing lists and its dependent streams")
        
        # Collect all parent streams that need to be loaded
        parent_streams_to_load = set()
        
        for stream_id in ctx.selected_stream_ids:
            if stream_id in lists_dependent_streams:
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

    # In future we need to add streams independent of lists, 
    # we can add their sync calls in `else` block.

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

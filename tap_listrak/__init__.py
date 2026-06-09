#!/usr/bin/env python3
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from zeep.exceptions import Fault
from . import streams as streams_
from .context import Context
from . import schemas
from .http import ListrakForbiddenError

REQUIRED_CONFIG_KEYS = ["start_date", "username", "password"]
LOGGER = singer.get_logger()

# Maps each child stream to its direct parent stream.
# Used to cascade-remove children when a parent stream is inaccessible.
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

# Only 'lists' can be probed during discovery without needing data IDs from
# a prior API call. Child streams (messages, message_*, subscribed_contacts)
# all require ListID or MsgID obtained at sync time.
_PROBEABLE_STREAMS = {
    'lists': 'GetContactListCollection',
}


def check_credentials_are_authorized(ctx):
    """
    Probe the 'lists' stream via GetContactListCollection to verify the
    account credentials have read access to the Listrak SOAP API.

    A zeep.Fault exception indicates the credentials lack sufficient access.
    Raises ListrakForbiddenError if the probe fails.

    Since 'lists' is the root dependency for all streams, a failed probe
    means no data can be collected.
    """
    try:
        ctx.client.service.GetContactListCollection()
        LOGGER.info("Listrak credentials verified: 'lists' stream is accessible.")
    except Fault as e:
        raise ListrakForbiddenError(
            "Error: The account credentials supplied do not have 'read' access "
            "to the Listrak API. Data collection cannot be initiated: {}".format(e)
        ) from e


def _prune_inaccessible_children(stream_ids, inaccessible):
    """
    Remove child streams from stream_ids whose parent stream is inaccessible.
    Runs iteratively to handle multi-level cascading (lists → messages → message_*).
    Mutates stream_ids in place.
    """
    changed = True
    while changed:
        changed = False
        for child, parent in STREAM_DEPENDENCIES.items():
            if child in stream_ids and parent not in stream_ids:
                LOGGER.warning(
                    "Stream '%s' excluded from catalog because its parent "
                    "stream '%s' is not accessible.",
                    child,
                    parent,
                )
                stream_ids.discard(child)
                inaccessible.add(child)
                changed = True


def discover(ctx):
    inaccessible = set()
    accessible_stream_ids = set(schemas.stream_ids)

    # Probe root-level streams that can be checked without data IDs
    for stream_id, service_method in _PROBEABLE_STREAMS.items():
        try:
            getattr(ctx.client.service, service_method)()
        except Fault as e:
            LOGGER.warning(
                "Stream '%s' does not have read permission, excluding from catalog: %s",
                stream_id,
                e,
            )
            accessible_stream_ids.discard(stream_id)
            inaccessible.add(stream_id)

    _prune_inaccessible_children(accessible_stream_ids, inaccessible)

    if not accessible_stream_ids:
        raise ListrakForbiddenError(
            "HTTP-error-code: 403, Error: The account credentials supplied do not have "
            "'read' access to any of the streams supported by the tap. Data collection "
            "cannot be initiated due to lack of permissions."
        )

    if inaccessible:
        LOGGER.warning(
            "The account credentials supplied do not have 'read' access to the "
            "following stream(s): %s. These streams have been excluded from the catalog.",
            ", ".join(sorted(inaccessible)),
        )

    catalog = Catalog([])

    for tap_stream_id in schemas.stream_ids:
        if tap_stream_id not in accessible_stream_ids:
            continue

        schema_dict = schemas.load_schema(tap_stream_id)
        schema = Schema.from_dict(schema_dict)

        mdata = metadata.get_standard_metadata(
            schema_dict,
            replication_method=schemas.REPLICATION_METHODS[tap_stream_id],
            key_properties=schemas.PK_FIELDS[tap_stream_id]
        )

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
    """

    # All lists-dependent streams are synced through sync_lists
    LOGGER.info("Syncing lists and its dependent streams")

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

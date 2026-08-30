#!/usr/bin/env python3
import pendulum
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

# SOAP endpoints for streams requiring a MsgID.
_MESSAGE_SUBSTREAM_ENDPOINTS = {
    'message_clicks':  'ReportRangeMessageContactClick',
    'message_opens':   'ReportRangeMessageContactOpen',
    'message_reads':   'ReportRangeMessageContactRead',
    'message_unsubs':  'ReportRangeMessageContactRemoval',
    'message_bounces': 'ReportRangeMessageContactBounces',
    'message_sends':   'ReportMessageContactSent',
}


def _log_unauthorized_stream(stream_name, exc):
    LOGGER.warning(
        "Excluding unauthorized stream '%s' from catalog. HTTP-Error-Message: '%s'",
        stream_name,
        str(exc)
    )


def _build_catalog_metadata(schema_dict, tap_stream_id):
    mdata = metadata.get_standard_metadata(
        schema_dict,
        replication_method=schemas.REPLICATION_METHODS[tap_stream_id],
        key_properties=schemas.PK_FIELDS[tap_stream_id]
    )
    mdata = metadata.to_map(mdata)

    # `lists` and `messages` are required for their substreams.
    if tap_stream_id in ['lists', 'messages']:
        mdata = metadata.write(mdata, (), 'inclusion', 'automatic')

    for field_name in schema_dict['properties'].keys():
        mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')

    if parent_stream := STREAM_DEPENDENCIES.get(tap_stream_id):
        mdata = metadata.write(mdata, (), 'parent-tap-stream-id', parent_stream)

    return mdata


def get_schemas_with_metadata():
    """
    Return stream schemas and stream metadata keyed by stream id.
    """
    schema_map = {}
    field_metadata = {}

    for tap_stream_id in schemas.stream_ids:
        schema_dict = schemas.load_schema(tap_stream_id)
        schema_map[tap_stream_id] = schema_dict
        field_metadata[tap_stream_id] = _build_catalog_metadata(schema_dict, tap_stream_id)

    return schema_map, field_metadata


def check_credentials_are_authorized(ctx):
    """
    Probe the 'lists' stream via GetContactListCollection.
    Returns the first ListID from the response, or None when the account has
    no lists but the credentials are still authorized for the stream.
    Raises ListrakForbiddenError if credentials lack read access.
    """
    try:
        response = ctx.client.service.GetContactListCollection()
        LOGGER.info("Stream 'lists' is accessible.")
        lists = response or []
        if not lists:
            LOGGER.warning(
                "Stream 'lists' is accessible, but the account has no lists. "
                "Excluding dependent streams from catalog because no ListID is available."
            )
            return None
        return lists[0].ListID
    except Fault as exc:
        _log_unauthorized_stream('lists', exc)
        raise ListrakForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have "
            "'read' access to stream 'lists'. HTTP-Error-Message: '%s'" % str(exc)
        ) from exc


def _probe_list_dependent(ctx, stream_id, list_id):
    """
    Probe 'messages' or 'subscribed_contacts' using a real ListID.
    Uses a 365-day look-back to maximise the chance of finding message data.
    Returns (is_accessible, msg_id_or_None).
    """
    now = pendulum.now("UTC")
    start = now.subtract(days=365)
    try:
        if stream_id == 'messages':
            response = ctx.client.service.ReportListMessageActivity(
                ListID=list_id, StartDate=start, EndDate=now, IncludeTestMessages=True
            )
            LOGGER.info("Stream 'messages' is accessible.")
            msg_id = None
            try:
                act_result = response["ReportListMessageActivityResult"]
                ws_messages = act_result["WSMessageActivity"] if act_result else None
                if ws_messages:
                    msg_id = ws_messages[0]["MsgID"]
            except (TypeError, KeyError, IndexError):
                pass
            return True, msg_id

        if stream_id == 'subscribed_contacts':
            ctx.client.service.ReportRangeSubscribedContacts(
                ListID=list_id, StartDate=start, EndDate=now, Page=1
            )
            LOGGER.info("Stream 'subscribed_contacts' is accessible.")
            return True, None
    except Fault as exc:
        _log_unauthorized_stream(stream_id, exc)
        return False, None

    return False, None


def _probe_message_substream(ctx, stream_id, msg_id):
    """
    Probe a message_* sub-stream using a real MsgID.
    Returns True if accessible, False if a Fault is raised.
    """
    now = pendulum.now("UTC")
    start = now.subtract(days=365)
    endpoint = _MESSAGE_SUBSTREAM_ENDPOINTS[stream_id]
    kwargs = {'MsgID': msg_id, 'Page': 1}
    if stream_id != 'message_sends':
        kwargs.update({'StartDate': start, 'EndDate': now})
    try:
        getattr(ctx.client.service, endpoint)(**kwargs)
        LOGGER.info("Stream '%s' is accessible.", stream_id)
        return True
    except Fault as exc:
        _log_unauthorized_stream(stream_id, exc)
        return False


def _prune_inaccessible_children(schema_map, field_metadata):
    """
    Remove child streams when their parent stream is excluded.
    Mutates schema_map and field_metadata in place.
    """
    pruned_children = []
    changed = True
    while changed:
        changed = False
        for child, parent in STREAM_DEPENDENCIES.items():
            if child in schema_map and parent not in schema_map:
                LOGGER.warning(
                    "Stream '%s' excluded from catalog because its parent "
                    "stream '%s' is not accessible.",
                    child,
                    parent,
                )
                schema_map.pop(child, None)
                field_metadata.pop(child, None)
                pruned_children.append(child)
                changed = True

    return pruned_children


def _apply_access_checks(ctx, schema_map, field_metadata):
    """
    Probe stream access and remove inaccessible streams from discovery output.
    Mutates schema_map and field_metadata in place.
    """
    inaccessible_streams = []

    # Step 1: probe `lists`.
    list_id = check_credentials_are_authorized(ctx)

    if list_id is None:
        for stream_id in ('messages', 'subscribed_contacts'):
            if stream_id in schema_map:
                schema_map.pop(stream_id, None)
                field_metadata.pop(stream_id, None)
        _prune_inaccessible_children(schema_map, field_metadata)
        return

    # Step 2: probe list-dependent streams.
    messages_accessible, msg_id = _probe_list_dependent(ctx, 'messages', list_id)
    if not messages_accessible and 'messages' in schema_map:
        inaccessible_streams.append('messages')
        schema_map.pop('messages', None)
        field_metadata.pop('messages', None)

    contacts_accessible, _ = _probe_list_dependent(ctx, 'subscribed_contacts', list_id)
    if not contacts_accessible and 'subscribed_contacts' in schema_map:
        inaccessible_streams.append('subscribed_contacts')
        schema_map.pop('subscribed_contacts', None)
        field_metadata.pop('subscribed_contacts', None)

    # Step 3: probe message sub-streams when we have a probe MsgID.
    if messages_accessible and msg_id is not None:
        for stream_id in _MESSAGE_SUBSTREAM_ENDPOINTS:
            if stream_id in schema_map and not _probe_message_substream(ctx, stream_id, msg_id):
                inaccessible_streams.append(stream_id)
                schema_map.pop(stream_id, None)
                field_metadata.pop(stream_id, None)
    elif messages_accessible:
        LOGGER.warning(
            "No messages found in account history; message_* sub-streams "
            "included in catalog without access check."
        )

    inaccessible_streams.extend(_prune_inaccessible_children(schema_map, field_metadata))

    if not schema_map:
        raise ListrakForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' "
            "access to any supported streams."
        )

    if inaccessible_streams:
        LOGGER.warning(
            "Unauthorized streams excluded from catalog: %s",
            ", ".join(sorted(set(inaccessible_streams))),
        )


def discover(ctx):
    """
    Run discovery and exclude inaccessible streams from the generated catalog.
    """
    schema_map, field_metadata = get_schemas_with_metadata()
    _apply_access_checks(ctx, schema_map, field_metadata)

    catalog = Catalog([])
    for tap_stream_id in schemas.stream_ids:
        if tap_stream_id not in schema_map:
            continue

        schema_dict = schema_map[tap_stream_id]
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[tap_stream_id]

        catalog.streams.append(CatalogEntry(
            stream=tap_stream_id,
            tap_stream_id=tap_stream_id,
            key_properties=schemas.PK_FIELDS[tap_stream_id],
            schema=schema,
            metadata=metadata.to_list(mdata)
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

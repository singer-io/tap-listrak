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


def check_credentials_are_authorized(ctx):
    """
    Probe the 'lists' stream via GetContactListCollection.
    Returns the first ListID from the response, or None if the account has no lists.
    Raises ListrakForbiddenError if credentials lack read access.
    """
    try:
        response = ctx.client.service.GetContactListCollection()
        LOGGER.info("Stream 'lists' is accessible.")
        lists = response or []
        return lists[0].ListID if lists else None
    except Fault as e:
        raise ListrakForbiddenError(
            "HTTP-error-code: 403, Error: The account credentials supplied do not have "
            "'read' access to any of the streams supported by the tap. "
            "Data collection cannot be initiated: {}".format(e)
        ) from e


def _probe_list_dependent(ctx, stream_id, list_id):
    """
    Probe 'messages' or 'subscribed_contacts' using a real ListID.
    Uses a 365-day look-back to maximise the chance of finding message data.
    Returns (is_accessible, msg_id_or_None).
      - For 'messages': msg_id is the first MsgID found in the response (or None).
      - For 'subscribed_contacts': msg_id is always None.
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
        elif stream_id == 'subscribed_contacts':
            ctx.client.service.ReportRangeSubscribedContacts(
                ListID=list_id, StartDate=start, EndDate=now, Page=1
            )
            LOGGER.info("Stream 'subscribed_contacts' is accessible.")
            return True, None
    except Fault as e:
        LOGGER.warning(
            "Stream '%s' does not have read permission, excluding from catalog: %s",
            stream_id, e,
        )
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
    except Fault as e:
        LOGGER.warning(
            "Stream '%s' does not have read permission, excluding from catalog: %s",
            stream_id, e,
        )
        return False


def _prune_inaccessible_children(stream_ids, inaccessible):
    """
    Cascade-remove child streams whose parent was marked inaccessible.
    Used when messages was blocked (without a MsgID probe) to ensure
    all message_* streams are also removed.
    Runs iteratively to handle multi-level chains.
    """
    changed = True
    while changed:
        changed = False
        for child, parent in STREAM_DEPENDENCIES.items():
            if child in stream_ids and parent not in stream_ids:
                LOGGER.warning(
                    "Stream '%s' excluded from catalog because its parent "
                    "stream '%s' is not accessible.",
                    child, parent,
                )
                stream_ids.discard(child)
                inaccessible.add(child)
                changed = True


def discover(ctx):
    inaccessible = set()
    accessible_stream_ids = set(schemas.stream_ids)

    # Step 1: probe 'lists' — raises immediately if inaccessible.
    list_id = check_credentials_are_authorized(ctx)

    if list_id is not None:
        # Step 2: probe 'messages' and 'subscribed_contacts' with the ListID.
        messages_accessible, msg_id = _probe_list_dependent(ctx, 'messages', list_id)
        if not messages_accessible:
            accessible_stream_ids.discard('messages')
            inaccessible.add('messages')

        sc_accessible, _ = _probe_list_dependent(ctx, 'subscribed_contacts', list_id)
        if not sc_accessible:
            accessible_stream_ids.discard('subscribed_contacts')
            inaccessible.add('subscribed_contacts')

        # Step 3: probe message_* sub-streams with the MsgID.
        if messages_accessible and msg_id is not None:
            for stream_id in _MESSAGE_SUBSTREAM_ENDPOINTS:
                if not _probe_message_substream(ctx, stream_id, msg_id):
                    accessible_stream_ids.discard(stream_id)
                    inaccessible.add(stream_id)
        elif messages_accessible:
            LOGGER.warning(
                "No messages found in account history; skipping access check for "
                "message_* sub-streams — they will be included in the catalog."
            )
    else:
        LOGGER.warning(
            "No lists found in the account; skipping access check for child streams."
        )

    # Cascade: remove message_* children if messages was excluded.
    _prune_inaccessible_children(accessible_stream_ids, inaccessible)

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

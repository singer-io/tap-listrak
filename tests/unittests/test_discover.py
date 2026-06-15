import unittest
from unittest.mock import MagicMock, patch
from zeep.exceptions import Fault
from tap_listrak import schemas, discover
from tap_listrak.__init__ import (
    check_credentials_are_authorized,
    _probe_list_dependent,
    _probe_message_substream,
    _MESSAGE_SUBSTREAM_ENDPOINTS,
)
from tap_listrak.http import ListrakForbiddenError
from singer import metadata


# ---------------------------------------------------------------------------
# Note: existing catalog-structure test classes use MagicMock() (no spec) so
# that ctx.client is auto-created and the SOAP probe in discover() succeeds.
# ---------------------------------------------------------------------------


class TestDiscoverMetadata(unittest.TestCase):
    """Test discover function metadata generation."""

    def setUp(self):
        """Set up test fixtures."""
        self.ctx = MagicMock()  # no spec: client attribute auto-created for SOAP probe
        self.ctx.config = {
            'start_date': '2026-01-01T00:00:00Z',
            'username': 'test_user',
            'password': 'test_pass'
        }

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_returns_catalog(self, mock_load_schema):
        """Test that discover returns a Catalog object with correct streams."""
        # Mock schema for a simple stream
        mock_load_schema.return_value = {
            'type': 'object',
            'properties': {
                'ListID': {'type': 'integer'},
                'Name': {'type': 'string'}
            }
        }

        catalog = discover(self.ctx)

        # Verify catalog is returned
        self.assertIsNotNone(catalog)
        # Verify all streams are present
        self.assertEqual(len(catalog.streams), len(schemas.stream_ids))

    @patch('singer.metadata.get_standard_metadata')
    @patch('tap_listrak.schemas.load_schema')
    def test_discover_calls_get_standard_metadata_with_replication_method(self, mock_load_schema, mock_get_standard_metadata):
        """Test that discover calls get_standard_metadata with correct replication_method."""
        mock_load_schema.return_value = {
            'type': 'object',
            'properties': {
                'ListID': {'type': 'integer'}
            }
        }

        # Mock get_standard_metadata to return a basic metadata list
        mock_get_standard_metadata.return_value = [
            {'breadcrumb': (), 'metadata': {}}
        ]

        catalog = discover(self.ctx)

        # Verify get_standard_metadata was called with FULL_TABLE for each stream
        for call in mock_get_standard_metadata.call_args_list:
            kwargs = call[1]
            self.assertEqual(kwargs['replication_method'], 'FULL_TABLE')

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_sets_key_properties(self, mock_load_schema):
        """Test that discover sets key_properties in catalog entries."""
        mock_load_schema.return_value = {
            'type': 'object',
            'properties': {'ListID': {'type': 'integer'}}
        }

        catalog = discover(self.ctx)

        # Verify key properties for specific streams
        lists_stream = next(s for s in catalog.streams if s.tap_stream_id == 'lists')
        self.assertEqual(lists_stream.key_properties, ['ListID'])

        messages_stream = next(s for s in catalog.streams if s.tap_stream_id == 'messages')
        self.assertEqual(messages_stream.key_properties, ['MsgID'])

        clicks_stream = next(s for s in catalog.streams if s.tap_stream_id == 'message_clicks')
        self.assertEqual(clicks_stream.key_properties, ['MsgID', 'EmailAddress'])

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_sets_automatic_inclusion_for_parent_streams(self, mock_load_schema):
        """Test that lists and messages have automatic inclusion metadata."""
        mock_load_schema.return_value = {
            'type': 'object',
            'properties': {'ListID': {'type': 'integer'}}
        }

        catalog = discover(self.ctx)

        # Check lists stream has automatic inclusion
        lists_stream = next(s for s in catalog.streams if s.tap_stream_id == 'lists')
        lists_mdata = metadata.to_map(lists_stream.metadata)
        inclusion = metadata.get(lists_mdata, (), 'inclusion')
        self.assertEqual(inclusion, 'automatic')

        # Check messages stream has automatic inclusion
        messages_stream = next(s for s in catalog.streams if s.tap_stream_id == 'messages')
        messages_mdata = metadata.to_map(messages_stream.metadata)
        inclusion = metadata.get(messages_mdata, (), 'inclusion')
        self.assertEqual(inclusion, 'automatic')

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_sets_automatic_inclusion_for_all_fields(self, mock_load_schema):
        """Test that all fields have automatic inclusion metadata."""
        mock_load_schema.return_value = {
            'type': 'object',
            'properties': {
                'ListID': {'type': 'integer'},
                'Name': {'type': 'string'},
                'CreatedDate': {'type': 'string'}
            }
        }

        catalog = discover(self.ctx)

        # Check a stream's fields have automatic inclusion
        lists_stream = next(s for s in catalog.streams if s.tap_stream_id == 'lists')
        lists_mdata = metadata.to_map(lists_stream.metadata)

        # Verify all fields have automatic inclusion
        for field_name in ['ListID', 'Name', 'CreatedDate']:
            field_inclusion = metadata.get(lists_mdata, ('properties', field_name), 'inclusion')
            self.assertEqual(field_inclusion, 'automatic',
                           f"Field {field_name} should have automatic inclusion")

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_sets_parent_stream_id_for_child_streams(self, mock_load_schema):
        """Test that child streams have parent-tap-stream-id metadata."""
        mock_load_schema.return_value = {
            'type': 'object',
            'properties': {'MsgID': {'type': 'integer'}}
        }

        catalog = discover(self.ctx)

        # Check messages stream has lists as parent
        messages_stream = next(s for s in catalog.streams if s.tap_stream_id == 'messages')
        messages_mdata = metadata.to_map(messages_stream.metadata)
        parent = metadata.get(messages_mdata, (), 'parent-tap-stream-id')
        self.assertEqual(parent, 'lists')

        # Check subscribed_contacts has lists as parent
        contacts_stream = next(s for s in catalog.streams if s.tap_stream_id == 'subscribed_contacts')
        contacts_mdata = metadata.to_map(contacts_stream.metadata)
        parent = metadata.get(contacts_mdata, (), 'parent-tap-stream-id')
        self.assertEqual(parent, 'lists')

        # Check message_clicks has messages as parent
        clicks_stream = next(s for s in catalog.streams if s.tap_stream_id == 'message_clicks')
        clicks_mdata = metadata.to_map(clicks_stream.metadata)
        parent = metadata.get(clicks_mdata, (), 'parent-tap-stream-id')
        self.assertEqual(parent, 'messages')

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_no_parent_for_top_level_streams(self, mock_load_schema):
        """Test that top-level streams (lists) don't have parent-tap-stream-id."""
        mock_load_schema.return_value = {
            'type': 'object',
            'properties': {'ListID': {'type': 'integer'}}
        }

        catalog = discover(self.ctx)

        # Check lists stream has no parent
        lists_stream = next(s for s in catalog.streams if s.tap_stream_id == 'lists')
        lists_mdata = metadata.to_map(lists_stream.metadata)
        parent = metadata.get(lists_mdata, (), 'parent-tap-stream-id')
        self.assertIsNone(parent)

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_all_message_substreams_have_messages_parent(self, mock_load_schema):
        """Test that all message sub-streams have messages as parent."""
        mock_load_schema.return_value = {
            'type': 'object',
            'properties': {'MsgID': {'type': 'integer'}}
        }

        catalog = discover(self.ctx)

        message_substreams = [
            'message_clicks',
            'message_opens',
            'message_reads',
            'message_sends',
            'message_unsubs',
            'message_bounces'
        ]

        for stream_id in message_substreams:
            stream = next(s for s in catalog.streams if s.tap_stream_id == stream_id)
            mdata = metadata.to_map(stream.metadata)
            parent = metadata.get(mdata, (), 'parent-tap-stream-id')
            self.assertEqual(parent, 'messages',
                           f"{stream_id} should have messages as parent")

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_catalog_entry_structure(self, mock_load_schema):
        """Test that catalog entries have all required fields."""
        mock_load_schema.return_value = {
            'type': 'object',
            'properties': {'ListID': {'type': 'integer'}}
        }

        catalog = discover(self.ctx)

        for stream in catalog.streams:
            # Verify required fields exist
            self.assertIsNotNone(stream.stream)
            self.assertIsNotNone(stream.tap_stream_id)
            self.assertIsNotNone(stream.key_properties)
            self.assertIsNotNone(stream.schema)
            self.assertIsNotNone(stream.metadata)

            # Verify tap_stream_id matches stream
            self.assertEqual(stream.stream, stream.tap_stream_id)

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_metadata_is_list_format(self, mock_load_schema):
        """Test that metadata is in list format (not map)."""
        mock_load_schema.return_value = {
            'type': 'object',
            'properties': {'ListID': {'type': 'integer'}}
        }

        catalog = discover(self.ctx)

        for stream in catalog.streams:
            # Metadata should be a list
            self.assertIsInstance(stream.metadata, list)

            # Each metadata entry should have breadcrumb and metadata keys
            for entry in stream.metadata:
                self.assertIn('breadcrumb', entry)
                self.assertIn('metadata', entry)

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_loads_schema_for_all_streams(self, mock_load_schema):
        """Test that discover loads schema for every stream."""
        mock_load_schema.return_value = {
            'type': 'object',
            'properties': {'id': {'type': 'integer'}}
        }

        discover(self.ctx)

        # Verify load_schema was called for each stream
        self.assertEqual(mock_load_schema.call_count, len(schemas.stream_ids))

        # Verify it was called with each stream_id
        called_stream_ids = [call[0][0] for call in mock_load_schema.call_args_list]
        for stream_id in schemas.stream_ids:
            self.assertIn(stream_id, called_stream_ids)


class TestStreamDependencyHierarchy(unittest.TestCase):
    """Test the dependency hierarchy is correctly represented in metadata."""

    @patch('tap_listrak.schemas.load_schema')
    def test_two_level_dependency_hierarchy(self, mock_load_schema):
        """Test lists -> messages -> message_clicks dependency chain."""
        mock_load_schema.return_value = {
            'type': 'object',
            'properties': {'id': {'type': 'integer'}}
        }

        ctx = MagicMock()  # no spec: client auto-created for SOAP probe
        catalog = discover(ctx)

        # lists has no parent
        lists_stream = next(s for s in catalog.streams if s.tap_stream_id == 'lists')
        lists_mdata = metadata.to_map(lists_stream.metadata)
        self.assertIsNone(metadata.get(lists_mdata, (), 'parent-tap-stream-id'))

        # messages has lists as parent
        messages_stream = next(s for s in catalog.streams if s.tap_stream_id == 'messages')
        messages_mdata = metadata.to_map(messages_stream.metadata)
        self.assertEqual(metadata.get(messages_mdata, (), 'parent-tap-stream-id'), 'lists')

        # message_clicks has messages as parent (not lists, even though it's a grandchild)
        clicks_stream = next(s for s in catalog.streams if s.tap_stream_id == 'message_clicks')
        clicks_mdata = metadata.to_map(clicks_stream.metadata)
        self.assertEqual(metadata.get(clicks_mdata, (), 'parent-tap-stream-id'), 'messages')

    @patch('tap_listrak.schemas.load_schema')
    def test_automatic_inclusion_only_for_direct_parents(self, mock_load_schema):
        """Test that only lists and messages have automatic inclusion, not child streams."""
        mock_load_schema.return_value = {
            'type': 'object',
            'properties': {'id': {'type': 'integer'}}
        }

        ctx = MagicMock()  # no spec: client auto-created for SOAP probe
        catalog = discover(ctx)

        # lists and messages should have automatic inclusion
        automatic_streams = ['lists', 'messages']
        for stream_id in automatic_streams:
            stream = next(s for s in catalog.streams if s.tap_stream_id == stream_id)
            mdata = metadata.to_map(stream.metadata)
            inclusion = metadata.get(mdata, (), 'inclusion')
            self.assertEqual(inclusion, 'automatic',
                           f"{stream_id} should have automatic inclusion")

        # Child streams should NOT have automatic inclusion at stream level
        child_streams = ['message_clicks', 'subscribed_contacts']
        for stream_id in child_streams:
            stream = next(s for s in catalog.streams if s.tap_stream_id == stream_id)
            mdata = metadata.to_map(stream.metadata)
            inclusion = metadata.get(mdata, (), 'inclusion')
            # They should not have 'automatic' at stream level (only at field level)
            # The stream-level inclusion should be None or 'available'
            self.assertNotEqual(inclusion, 'automatic',
                              f"{stream_id} should not have automatic stream-level inclusion")


# ---------------------------------------------------------------------------
# Tests for check_credentials_are_authorized
# ---------------------------------------------------------------------------

class TestCheckCredentialsAuthorized(unittest.TestCase):

    def _make_ctx(self, fault=None, list_id=42):
        ctx = MagicMock()
        if fault:
            ctx.client.service.GetContactListCollection.side_effect = fault
        else:
            mock_list = MagicMock()
            mock_list.ListID = list_id
            ctx.client.service.GetContactListCollection.return_value = [mock_list]
        return ctx

    def test_returns_list_id_when_lists_accessible(self):
        ctx = self._make_ctx(list_id=7)
        result = check_credentials_are_authorized(ctx)
        self.assertEqual(result, 7)

    def test_raises_when_lists_empty(self):
        ctx = MagicMock()
        ctx.client.service.GetContactListCollection.return_value = []
        with self.assertRaises(ListrakForbiddenError) as cm:
            check_credentials_are_authorized(ctx)
        self.assertIn("403", str(cm.exception))

    def test_raises_forbidden_on_fault(self):
        ctx = self._make_ctx(fault=Fault("Access denied"))
        with self.assertRaises(ListrakForbiddenError):
            check_credentials_are_authorized(ctx)

    def test_forbidden_error_contains_403_and_fault_message(self):
        ctx = self._make_ctx(fault=Fault("InvalidLogonAttempt"))
        with self.assertRaises(ListrakForbiddenError) as cm:
            check_credentials_are_authorized(ctx)
        self.assertIn("403", str(cm.exception))
        self.assertIn("InvalidLogonAttempt", str(cm.exception))

    def test_probes_get_contact_list_collection(self):
        ctx = self._make_ctx()
        check_credentials_are_authorized(ctx)
        ctx.client.service.GetContactListCollection.assert_called_once()


# ---------------------------------------------------------------------------
# Tests for discover() access-check integration
# ---------------------------------------------------------------------------

class TestDiscoverAccessChecks(unittest.TestCase):

    _SIMPLE_SCHEMA = {'type': 'object', 'properties': {'ListID': {'type': 'integer'}}}

    def _make_ctx(self, fault_on_lists=False, fault_on_messages=False,
                  fault_on_subscribed_contacts=False,
                  fault_on_message_substreams=None, list_id=42, msg_id=77):
        """
        Build a mock context with configurable fault injection.
        GetContactListCollection returns one list with ListID=list_id.
        ReportListMessageActivity returns a response containing msg_id.
        """
        ctx = MagicMock()
        if fault_on_lists:
            ctx.client.service.GetContactListCollection.side_effect = Fault("Access denied")
        else:
            mock_list = MagicMock()
            mock_list.ListID = list_id
            ctx.client.service.GetContactListCollection.return_value = [mock_list]

        if fault_on_messages:
            ctx.client.service.ReportListMessageActivity.side_effect = Fault("Access denied")
        else:
            mock_msg = MagicMock()
            mock_msg.__getitem__ = lambda s, k: msg_id if k == 'MsgID' else None
            ctx.client.service.ReportListMessageActivity.return_value = {
                'ReportListMessageActivityResult': {'WSMessageActivity': [mock_msg]}
            }

        if fault_on_subscribed_contacts:
            ctx.client.service.ReportRangeSubscribedContacts.side_effect = Fault("Access denied")

        for stream_id, endpoint in _MESSAGE_SUBSTREAM_ENDPOINTS.items():
            if stream_id in (fault_on_message_substreams or []):
                getattr(ctx.client.service, endpoint).side_effect = Fault("Access denied")

        return ctx

    @patch('tap_listrak.schemas.load_schema')
    def test_all_streams_included_when_all_accessible(self, mock_load_schema):
        mock_load_schema.return_value = self._SIMPLE_SCHEMA
        ctx = self._make_ctx()
        catalog = discover(ctx)
        self.assertEqual(
            {s.tap_stream_id for s in catalog.streams}, set(schemas.stream_ids)
        )

    @patch('tap_listrak.schemas.load_schema')
    def test_raises_when_lists_inaccessible(self, mock_load_schema):
        mock_load_schema.return_value = self._SIMPLE_SCHEMA
        ctx = self._make_ctx(fault_on_lists=True)
        with self.assertRaises(ListrakForbiddenError) as cm:
            discover(ctx)
        self.assertIn("403", str(cm.exception))

    @patch('tap_listrak.schemas.load_schema')
    def test_messages_and_all_substreams_excluded_when_messages_forbidden(self, mock_load_schema):
        mock_load_schema.return_value = self._SIMPLE_SCHEMA
        ctx = self._make_ctx(fault_on_messages=True)
        catalog = discover(ctx)
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        for excluded in ('messages', 'message_clicks', 'message_opens', 'message_reads',
                         'message_sends', 'message_unsubs', 'message_bounces'):
            self.assertNotIn(excluded, stream_ids)
        self.assertIn('lists', stream_ids)
        self.assertIn('subscribed_contacts', stream_ids)

    @patch('tap_listrak.schemas.load_schema')
    def test_subscribed_contacts_excluded_when_forbidden(self, mock_load_schema):
        mock_load_schema.return_value = self._SIMPLE_SCHEMA
        ctx = self._make_ctx(fault_on_subscribed_contacts=True)
        catalog = discover(ctx)
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn('subscribed_contacts', stream_ids)
        self.assertIn('lists', stream_ids)
        self.assertIn('messages', stream_ids)

    @patch('tap_listrak.schemas.load_schema')
    def test_individual_message_substream_excluded_when_forbidden(self, mock_load_schema):
        mock_load_schema.return_value = self._SIMPLE_SCHEMA
        ctx = self._make_ctx(fault_on_message_substreams=['message_clicks'])
        catalog = discover(ctx)
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn('message_clicks', stream_ids)
        self.assertIn('message_opens', stream_ids)
        self.assertIn('messages', stream_ids)

    @patch('tap_listrak.schemas.load_schema')
    def test_messages_probed_with_correct_list_id(self, mock_load_schema):
        mock_load_schema.return_value = self._SIMPLE_SCHEMA
        ctx = self._make_ctx(list_id=99)
        discover(ctx)
        self.assertEqual(
            ctx.client.service.ReportListMessageActivity.call_args[1]['ListID'], 99
        )

    @patch('tap_listrak.schemas.load_schema')
    def test_subscribed_contacts_probed_with_correct_list_id(self, mock_load_schema):
        mock_load_schema.return_value = self._SIMPLE_SCHEMA
        ctx = self._make_ctx(list_id=99)
        discover(ctx)
        self.assertEqual(
            ctx.client.service.ReportRangeSubscribedContacts.call_args[1]['ListID'], 99
        )

    @patch('tap_listrak.schemas.load_schema')
    def test_message_substreams_probed_with_correct_msg_id(self, mock_load_schema):
        mock_load_schema.return_value = self._SIMPLE_SCHEMA
        ctx = self._make_ctx(msg_id=55)
        discover(ctx)
        for stream_id, endpoint in _MESSAGE_SUBSTREAM_ENDPOINTS.items():
            svc = getattr(ctx.client.service, endpoint)
            self.assertEqual(
                svc.call_args[1]['MsgID'], 55,
                f"{stream_id} should be probed with MsgID=55"
            )

    @patch('tap_listrak.schemas.load_schema')
    def test_message_substreams_skipped_when_no_msg_id(self, mock_load_schema):
        """If messages returns no data, message_* probes are skipped and streams kept."""
        mock_load_schema.return_value = self._SIMPLE_SCHEMA
        ctx = MagicMock()
        mock_list = MagicMock()
        mock_list.ListID = 1
        ctx.client.service.GetContactListCollection.return_value = [mock_list]
        ctx.client.service.ReportListMessageActivity.return_value = {
            'ReportListMessageActivityResult': None
        }
        catalog = discover(ctx)
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(stream_ids, set(schemas.stream_ids))

    @patch('tap_listrak.schemas.load_schema')
    def test_raises_when_no_lists_in_account(self, mock_load_schema):
        """Empty GetContactListCollection raises ListrakForbiddenError."""
        mock_load_schema.return_value = self._SIMPLE_SCHEMA
        ctx = MagicMock()
        ctx.client.service.GetContactListCollection.return_value = []
        with self.assertRaises(ListrakForbiddenError) as cm:
            discover(ctx)
        self.assertIn("403", str(cm.exception))

    @patch('tap_listrak.schemas.load_schema')
    def test_schema_loaded_only_for_accessible_streams(self, mock_load_schema):
        mock_load_schema.return_value = self._SIMPLE_SCHEMA
        ctx = self._make_ctx(fault_on_messages=True)
        discover(ctx)
        loaded = {c[0][0] for c in mock_load_schema.call_args_list}
        for excluded in ('messages', 'message_clicks', 'message_opens', 'message_reads',
                         'message_sends', 'message_unsubs', 'message_bounces'):
            self.assertNotIn(excluded, loaded)


    @patch('tap_listrak.schemas.load_schema')
    def test_discover_probes_lists_endpoint(self, mock_load_schema):
        mock_load_schema.return_value = self._SIMPLE_SCHEMA
        ctx = self._make_ctx()
        discover(ctx)
        ctx.client.service.GetContactListCollection.assert_called_once()

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_loads_schema_only_for_accessible_streams(self, mock_load_schema):
        """Schema is loaded only for streams that remain in the catalog."""
        mock_load_schema.return_value = self._SIMPLE_SCHEMA
        ctx = self._make_ctx(fault_on_messages=True)
        discover(ctx)
        loaded_ids = {call[0][0] for call in mock_load_schema.call_args_list}
        for excluded in ('messages', 'message_clicks', 'message_opens',
                         'message_reads', 'message_sends', 'message_unsubs',
                         'message_bounces'):
            self.assertNotIn(excluded, loaded_ids)


import unittest
from unittest.mock import MagicMock, patch, call
from zeep.exceptions import Fault
from tap_listrak import schemas, discover
from tap_listrak.__init__ import (
    STREAM_DEPENDENCIES,
    _PROBEABLE_STREAMS,
    _prune_inaccessible_children,
    check_credentials_are_authorized,
)
from tap_listrak.http import ListrakForbiddenError
from tap_listrak.context import Context
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

    def _make_ctx(self, fault=None):
        ctx = MagicMock()  # no spec: client attribute auto-created
        if fault:
            ctx.client.service.GetContactListCollection.side_effect = fault
        return ctx

    def test_succeeds_when_lists_accessible(self):
        """No exception raised when GetContactListCollection returns successfully."""
        ctx = self._make_ctx()
        try:
            check_credentials_are_authorized(ctx)
        except ListrakForbiddenError:
            self.fail("check_credentials_are_authorized raised unexpectedly")

    def test_raises_forbidden_on_fault(self):
        """ListrakForbiddenError raised when SOAP service returns a Fault."""
        ctx = self._make_ctx(fault=Fault("Access denied"))
        with self.assertRaises(ListrakForbiddenError):
            check_credentials_are_authorized(ctx)

    def test_forbidden_error_contains_fault_message(self):
        """The raised error message includes the original Fault message."""
        ctx = self._make_ctx(fault=Fault("InvalidLogonAttempt"))
        with self.assertRaises(ListrakForbiddenError) as cm:
            check_credentials_are_authorized(ctx)
        self.assertIn("InvalidLogonAttempt", str(cm.exception))

    def test_probes_get_contact_list_collection(self):
        """Verifies the exact SOAP method used for the access probe."""
        ctx = self._make_ctx()
        check_credentials_are_authorized(ctx)
        ctx.client.service.GetContactListCollection.assert_called_once()


# ---------------------------------------------------------------------------
# Tests for _prune_inaccessible_children
# ---------------------------------------------------------------------------

class TestPruneInaccessibleChildren(unittest.TestCase):

    def test_messages_removed_when_lists_inaccessible(self):
        accessible = set(schemas.stream_ids) - {'lists'}
        inaccessible = {'lists'}
        _prune_inaccessible_children(accessible, inaccessible)
        self.assertNotIn('messages', accessible)
        self.assertNotIn('subscribed_contacts', accessible)

    def test_message_substreams_cascade_when_lists_inaccessible(self):
        """Removing lists cascades to messages, then to all message_* streams."""
        accessible = set(schemas.stream_ids) - {'lists'}
        inaccessible = {'lists'}
        _prune_inaccessible_children(accessible, inaccessible)
        for child in ('message_clicks', 'message_opens', 'message_reads',
                      'message_sends', 'message_unsubs', 'message_bounces'):
            self.assertNotIn(child, accessible)

    def test_message_substreams_removed_when_messages_inaccessible(self):
        accessible = set(schemas.stream_ids) - {'messages'}
        inaccessible = {'messages'}
        _prune_inaccessible_children(accessible, inaccessible)
        for child in ('message_clicks', 'message_opens', 'message_reads',
                      'message_sends', 'message_unsubs', 'message_bounces'):
            self.assertNotIn(child, accessible)

    def test_lists_kept_when_no_parent_absent(self):
        accessible = set(schemas.stream_ids)
        inaccessible = set()
        _prune_inaccessible_children(accessible, inaccessible)
        self.assertIn('lists', accessible)
        self.assertIn('messages', accessible)

    def test_pruned_streams_added_to_inaccessible(self):
        accessible = set(schemas.stream_ids) - {'lists'}
        inaccessible = {'lists'}
        _prune_inaccessible_children(accessible, inaccessible)
        self.assertIn('messages', inaccessible)
        self.assertIn('subscribed_contacts', inaccessible)


# ---------------------------------------------------------------------------
# Tests for discover() access-check integration
# ---------------------------------------------------------------------------

class TestDiscoverAccessChecks(unittest.TestCase):

    def _make_ctx(self, fault_on_lists=False):
        ctx = MagicMock()  # no spec: client attribute auto-created
        if fault_on_lists:
            ctx.client.service.GetContactListCollection.side_effect = Fault("Access denied")
        return ctx

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_all_streams_when_lists_accessible(self, mock_load_schema):
        mock_load_schema.return_value = {'type': 'object', 'properties': {'ListID': {'type': 'integer'}}}
        ctx = self._make_ctx()
        catalog = discover(ctx)
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(stream_ids, set(schemas.stream_ids))

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_raises_when_lists_inaccessible(self, mock_load_schema):
        mock_load_schema.return_value = {'type': 'object', 'properties': {'ListID': {'type': 'integer'}}}
        ctx = self._make_ctx(fault_on_lists=True)
        with self.assertRaises(ListrakForbiddenError):
            discover(ctx)

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_excludes_lists_and_all_children_when_forbidden(self, mock_load_schema):
        """When lists is forbidden, catalog must be empty → ListrakForbiddenError raised."""
        mock_load_schema.return_value = {'type': 'object', 'properties': {'ListID': {'type': 'integer'}}}
        ctx = self._make_ctx(fault_on_lists=True)
        with self.assertRaises(ListrakForbiddenError) as cm:
            discover(ctx)
        self.assertIn("403", str(cm.exception))

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_probes_lists_endpoint(self, mock_load_schema):
        mock_load_schema.return_value = {'type': 'object', 'properties': {'ListID': {'type': 'integer'}}}
        ctx = self._make_ctx()
        discover(ctx)
        ctx.client.service.GetContactListCollection.assert_called_once()

    @patch('tap_listrak.schemas.load_schema')
    def test_discover_does_not_skip_schema_load_for_accessible_streams(self, mock_load_schema):
        """All accessible stream schemas must be loaded during discovery."""
        mock_load_schema.return_value = {'type': 'object', 'properties': {'id': {'type': 'integer'}}}
        ctx = self._make_ctx()
        discover(ctx)
        self.assertEqual(mock_load_schema.call_count, len(schemas.stream_ids))


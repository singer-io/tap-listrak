"""Integration tests for tap-listrak stream discovery with mocked data."""
import unittest
from unittest.mock import MagicMock
from singer import metadata

from .base import ListrakBaseTest

from tap_listrak import discover
from tap_listrak.context import Context


class ListrakDiscoveryTest(ListrakBaseTest, unittest.TestCase):

    def _get_catalog(self):
        """Helper to run discover with a mocked Context."""
        ctx = MagicMock(spec=Context)
        ctx.config = self.get_mock_config()
        return discover(ctx)

    def test_discovery_returns_all_expected_streams(self):
        """Verify that discover returns catalog entries for all expected streams."""
        catalog = self._get_catalog()
        expected = self.expected_metadata()

        discovered_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(discovered_ids, set(expected.keys()))

    def test_discovery_primary_keys(self):
        """Verify primary keys match expected for every stream."""
        catalog = self._get_catalog()
        expected = self.expected_metadata()

        for stream in catalog.streams:
            with self.subTest(stream=stream.tap_stream_id):
                self.assertEqual(
                    set(stream.key_properties),
                    expected[stream.tap_stream_id][self.PRIMARY_KEYS],
                )

    def test_discovery_replication_method(self):
        """Verify every stream uses FULL_TABLE replication."""
        catalog = self._get_catalog()
        expected = self.expected_metadata()

        for stream in catalog.streams:
            with self.subTest(stream=stream.tap_stream_id):
                mdata = metadata.to_map(stream.metadata)
                replication_method = metadata.get(
                    mdata, (), 'forced-replication-method'
                ) or metadata.get(mdata, (), 'replication-method')
                self.assertEqual(
                    replication_method,
                    expected[stream.tap_stream_id][self.REPLICATION_METHOD],
                )

    def test_discovery_schema_has_properties(self):
        """Verify every discovered stream has a schema with properties."""
        catalog = self._get_catalog()

        for stream in catalog.streams:
            with self.subTest(stream=stream.tap_stream_id):
                schema_dict = stream.schema.to_dict()
                self.assertIn("properties", schema_dict)
                self.assertGreater(
                    len(schema_dict["properties"]), 0,
                    f"Stream {stream.tap_stream_id} schema has no properties",
                )

    def test_discovery_parent_stream_metadata(self):
        """Verify child streams have parent-tap-stream-id metadata set correctly."""
        catalog = self._get_catalog()
        expected = self.expected_metadata()

        for stream in catalog.streams:
            with self.subTest(stream=stream.tap_stream_id):
                mdata = metadata.to_map(stream.metadata)
                parent = metadata.get(mdata, (), "parent-tap-stream-id")
                expected_parent = expected[stream.tap_stream_id].get(self.PARENT)
                self.assertEqual(parent, expected_parent)

    def test_discovery_automatic_inclusion_for_parent_streams(self):
        """Verify lists and messages have automatic stream-level inclusion."""
        catalog = self._get_catalog()

        for parent_id in ["lists", "messages"]:
            with self.subTest(stream=parent_id):
                stream = next(
                    s for s in catalog.streams if s.tap_stream_id == parent_id
                )
                mdata = metadata.to_map(stream.metadata)
                inclusion = metadata.get(mdata, (), "inclusion")
                self.assertEqual(inclusion, "automatic")

    def test_discovery_catalog_entry_structure(self):
        """Verify every catalog entry has required fields."""
        catalog = self._get_catalog()

        for stream in catalog.streams:
            with self.subTest(stream=stream.tap_stream_id):
                self.assertIsNotNone(stream.stream)
                self.assertIsNotNone(stream.tap_stream_id)
                self.assertEqual(stream.stream, stream.tap_stream_id)
                self.assertIsNotNone(stream.key_properties)
                self.assertIsNotNone(stream.schema)
                self.assertIsNotNone(stream.metadata)
                self.assertIsInstance(stream.metadata, list)

    def test_discovery_metadata_list_format(self):
        """Verify metadata entries have breadcrumb and metadata keys."""
        catalog = self._get_catalog()

        for stream in catalog.streams:
            with self.subTest(stream=stream.tap_stream_id):
                for entry in stream.metadata:
                    self.assertIn("breadcrumb", entry)
                    self.assertIn("metadata", entry)

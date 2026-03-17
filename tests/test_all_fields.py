"""Integration tests for tap-listrak all fields replication with mocked data."""
import unittest

try:
    from .base import ListrakBaseTest
except ImportError:
    from base import ListrakBaseTest

from tap_listrak import schemas


class ListrakAllFieldsTest(ListrakBaseTest, unittest.TestCase):

    def test_all_stream_schemas_generate_valid_records(self):
        """Test that all expected streams have valid schemas that generate records with all properties."""
        expected = self.expected_metadata()

        for stream_name in expected:
            with self.subTest(stream=stream_name):
                schema = self._load_schema(stream_name)
                record = self._generate_value(schema, date_value="2026-02-01T00:00:00Z")

                self.assertIsInstance(record, dict)
                # Every property in the schema should be present in the generated record
                for prop in schema.get("properties", {}):
                    self.assertIn(
                        prop, record,
                        f"Property '{prop}' missing from generated record for {stream_name}",
                    )

    def test_all_stream_schemas_have_primary_keys(self):
        """Verify that every schema contains its declared primary key fields."""
        expected = self.expected_metadata()

        for stream_name, meta in expected.items():
            with self.subTest(stream=stream_name):
                schema = self._load_schema(stream_name)
                properties = schema.get("properties", {})
                for pk in meta[self.PRIMARY_KEYS]:
                    self.assertIn(
                        pk, properties,
                        f"PK '{pk}' not in schema properties for {stream_name}",
                    )

    def test_pk_fields_match_expected_metadata(self):
        """Verify schemas.PK_FIELDS matches the expected metadata primary keys."""
        expected = self.expected_metadata()

        for stream_name, meta in expected.items():
            with self.subTest(stream=stream_name):
                self.assertEqual(
                    set(schemas.PK_FIELDS[stream_name]),
                    meta[self.PRIMARY_KEYS],
                )

    def test_replication_methods_match_expected_metadata(self):
        """Verify schemas.REPLICATION_METHODS matches expected metadata."""
        expected = self.expected_metadata()

        for stream_name, meta in expected.items():
            with self.subTest(stream=stream_name):
                self.assertEqual(
                    schemas.REPLICATION_METHODS[stream_name],
                    meta[self.REPLICATION_METHOD],
                )

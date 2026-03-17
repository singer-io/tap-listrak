"""Integration tests for tap-listrak automatic fields selection with mocked data."""
import unittest
from unittest.mock import patch, MagicMock

try:
    from .base import ListrakBaseTest
except ImportError:
    from base import ListrakBaseTest

from tap_listrak import discover
from tap_listrak.context import Context
from singer import metadata


class ListrakAutomaticFieldsTest(ListrakBaseTest, unittest.TestCase):
    """Verify automatic fields (PKs and all fields) are properly defined."""

    @patch('tap_listrak.http.zeep')
    def _get_catalog(self, mock_zeep):
        """Helper to run discover with a mocked zeep client."""
        mock_zeep.Client.return_value = MagicMock()
        ctx = MagicMock(spec=Context)
        ctx.config = self.get_mock_config()
        return discover(ctx)

    def test_primary_keys_are_automatic(self):
        """Verify primary key fields have automatic inclusion."""
        catalog = self._get_catalog()
        expected = self.expected_metadata()

        for stream in catalog.streams:
            stream_name = stream.tap_stream_id
            mdata = metadata.to_map(stream.metadata)

            for pk in expected[stream_name][self.PRIMARY_KEYS]:
                with self.subTest(stream=stream_name, primary_key=pk):
                    inclusion = metadata.get(
                        mdata, ("properties", pk), "inclusion"
                    )
                    self.assertEqual(
                        inclusion, "automatic",
                        f"PK '{pk}' in {stream_name} should be automatic",
                    )

    def test_all_fields_are_automatic(self):
        """In tap-listrak all fields are set to automatic inclusion."""
        catalog = self._get_catalog()

        for stream in catalog.streams:
            mdata = metadata.to_map(stream.metadata)
            schema_dict = stream.schema.to_dict()

            for field_name in schema_dict.get("properties", {}):
                with self.subTest(stream=stream.tap_stream_id, field=field_name):
                    inclusion = metadata.get(
                        mdata, ("properties", field_name), "inclusion"
                    )
                    self.assertEqual(
                        inclusion, "automatic",
                        f"{stream.tap_stream_id}.{field_name} should be automatic",
                    )

    def test_parent_streams_have_automatic_stream_inclusion(self):
        """lists and messages should have automatic stream-level inclusion."""
        catalog = self._get_catalog()

        for parent_id in ["lists", "messages"]:
            with self.subTest(stream=parent_id):
                stream = next(
                    s for s in catalog.streams if s.tap_stream_id == parent_id
                )
                mdata = metadata.to_map(stream.metadata)
                inclusion = metadata.get(mdata, (), "inclusion")
                self.assertEqual(inclusion, "automatic")

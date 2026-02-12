import pytest
import os
import json
from unittest.mock import patch, mock_open, MagicMock
from tap_listrak import schemas


class TestIDS:
    """Test the IDS class contains all expected stream identifiers."""

    def test_ids_constants(self):
        """Test that IDS class has all expected stream ID constants."""
        assert schemas.IDS.LISTS == "lists"
        assert schemas.IDS.MESSAGES == "messages"
        assert schemas.IDS.MESSAGE_CLICKS == "message_clicks"
        assert schemas.IDS.MESSAGE_OPENS == "message_opens"
        assert schemas.IDS.MESSAGE_READS == "message_reads"
        assert schemas.IDS.MESSAGE_SENDS == "message_sends"
        assert schemas.IDS.MESSAGE_UNSUBS == "message_unsubs"
        assert schemas.IDS.MESSAGE_BOUNCES == "message_bounces"
        assert schemas.IDS.SUBSCRIBED_CONTACTS == "subscribed_contacts"


class TestStreamIds:
    """Test the stream_ids list generation."""

    def test_stream_ids_list(self):
        """Test that stream_ids contains all IDS attributes."""
        assert "lists" in schemas.stream_ids
        assert "messages" in schemas.stream_ids
        assert "message_clicks" in schemas.stream_ids
        assert "message_opens" in schemas.stream_ids
        assert "message_reads" in schemas.stream_ids
        assert "message_sends" in schemas.stream_ids
        assert "message_unsubs" in schemas.stream_ids
        assert "message_bounces" in schemas.stream_ids
        assert "subscribed_contacts" in schemas.stream_ids

    def test_stream_ids_count(self):
        """Test that stream_ids has the correct number of streams."""
        assert len(schemas.stream_ids) == 9


class TestPKFields:
    """Test the PK_FIELDS dictionary configuration."""

    def test_pk_fields_all_streams_defined(self):
        """Test that all stream IDs have defined primary keys."""
        for stream_id in schemas.stream_ids:
            assert stream_id in schemas.PK_FIELDS, f"Missing PK definition for {stream_id}"

    def test_pk_fields_lists(self):
        """Test primary key fields for lists stream."""
        assert schemas.PK_FIELDS[schemas.IDS.LISTS] == ["ListID"]

    def test_pk_fields_messages(self):
        """Test primary key fields for messages stream."""
        assert schemas.PK_FIELDS[schemas.IDS.MESSAGES] == ["MsgID"]

    def test_pk_fields_message_clicks(self):
        """Test primary key fields for message_clicks stream."""
        assert schemas.PK_FIELDS[schemas.IDS.MESSAGE_CLICKS] == ["MsgID", "EmailAddress"]

    def test_pk_fields_message_opens(self):
        """Test primary key fields for message_opens stream."""
        assert schemas.PK_FIELDS[schemas.IDS.MESSAGE_OPENS] == ["MsgID", "EmailAddress"]

    def test_pk_fields_message_reads(self):
        """Test primary key fields for message_reads stream."""
        assert schemas.PK_FIELDS[schemas.IDS.MESSAGE_READS] == ["MsgID", "EmailAddress"]

    def test_pk_fields_message_sends(self):
        """Test primary key fields for message_sends stream."""
        assert schemas.PK_FIELDS[schemas.IDS.MESSAGE_SENDS] == ["MsgID", "EmailAddress"]

    def test_pk_fields_message_unsubs(self):
        """Test primary key fields for message_unsubs stream."""
        assert schemas.PK_FIELDS[schemas.IDS.MESSAGE_UNSUBS] == ["MsgID", "EmailAddress"]

    def test_pk_fields_message_bounces(self):
        """Test primary key fields for message_bounces stream."""
        assert schemas.PK_FIELDS[schemas.IDS.MESSAGE_BOUNCES] == ["MsgID", "EmailAddress"]

    def test_pk_fields_subscribed_contacts(self):
        """Test primary key fields for subscribed_contacts stream."""
        assert schemas.PK_FIELDS[schemas.IDS.SUBSCRIBED_CONTACTS] == ["ListID", "ContactID"]


class TestGetAbsPath:
    """Test the get_abs_path function."""

    def test_get_abs_path_returns_absolute_path(self):
        """Test that get_abs_path returns an absolute path."""
        result = schemas.get_abs_path("test.json")
        assert os.path.isabs(result)

    def test_get_abs_path_joins_correctly(self):
        """Test that get_abs_path correctly joins paths."""
        result = schemas.get_abs_path("schemas/test.json")
        assert result.endswith("schemas/test.json") or result.endswith("schemas\\test.json")

    def test_get_abs_path_relative_to_schemas_module(self):
        """Test that get_abs_path is relative to schemas.py location."""
        result = schemas.get_abs_path("test.json")
        schemas_dir = os.path.dirname(os.path.realpath(schemas.__file__))
        expected = os.path.join(schemas_dir, "test.json")
        assert result == expected


class TestLoadSchema:
    """Test the load_schema function."""

    @patch('tap_listrak.schemas.utils.load_json')
    def test_load_schema_calls_load_json(self, mock_load_json):
        """Test that load_schema calls utils.load_json with correct path."""
        mock_load_json.return_value = {"type": "object"}

        result = schemas.load_schema("lists")

        # Check that load_json was called once
        assert mock_load_json.call_count == 1

        # Check that the path argument contains the stream ID
        call_args = mock_load_json.call_args[0][0]
        assert "schemas" in call_args or "schemas" in call_args
        assert "lists.json" in call_args

    @patch('tap_listrak.schemas.utils.load_json')
    def test_load_schema_returns_schema_dict(self, mock_load_json):
        """Test that load_schema returns the schema dictionary."""
        expected_schema = {
            "type": "object",
            "properties": {
                "ListID": {"type": "integer"}
            }
        }
        mock_load_json.return_value = expected_schema

        result = schemas.load_schema("lists")

        assert result == expected_schema

    @patch('tap_listrak.schemas.utils.load_json')
    def test_load_schema_formats_path_correctly(self, mock_load_json):
        """Test that load_schema formats the path with .json extension."""
        mock_load_json.return_value = {}

        schemas.load_schema("messages")

        call_args = mock_load_json.call_args[0][0]
        assert call_args.endswith("messages.json")


class TestLoadAndWriteSchema:
    """Test the load_and_write_schema function."""

    @patch('tap_listrak.schemas.singer.write_schema')
    @patch('tap_listrak.schemas.load_schema')
    def test_load_and_write_schema_loads_schema(self, mock_load_schema, mock_write_schema):
        """Test that load_and_write_schema loads the schema."""
        mock_schema = {"type": "object"}
        mock_load_schema.return_value = mock_schema

        schemas.load_and_write_schema("lists")

        mock_load_schema.assert_called_once_with("lists")

    @patch('tap_listrak.schemas.singer.write_schema')
    @patch('tap_listrak.schemas.load_schema')
    def test_load_and_write_schema_writes_schema(self, mock_load_schema, mock_write_schema):
        """Test that load_and_write_schema writes the schema with correct parameters."""
        mock_schema = {"type": "object"}
        mock_load_schema.return_value = mock_schema

        schemas.load_and_write_schema("lists")

        mock_write_schema.assert_called_once_with(
            "lists",
            mock_schema,
            ["ListID"]
        )

    @patch('tap_listrak.schemas.singer.write_schema')
    @patch('tap_listrak.schemas.load_schema')
    def test_load_and_write_schema_with_composite_keys(self, mock_load_schema, mock_write_schema):
        """Test load_and_write_schema with streams that have composite keys."""
        mock_schema = {"type": "object"}
        mock_load_schema.return_value = mock_schema

        schemas.load_and_write_schema("message_clicks")

        mock_write_schema.assert_called_once_with(
            "message_clicks",
            mock_schema,
            ["MsgID", "EmailAddress"]
        )

    @patch('tap_listrak.schemas.singer.write_schema')
    @patch('tap_listrak.schemas.load_schema')
    def test_load_and_write_schema_all_streams(self, mock_load_schema, mock_write_schema):
        """Test load_and_write_schema works for all defined streams."""
        mock_schema = {"type": "object"}
        mock_load_schema.return_value = mock_schema

        for stream_id in schemas.stream_ids:
            mock_write_schema.reset_mock()
            mock_load_schema.reset_mock()

            schemas.load_and_write_schema(stream_id)

            mock_load_schema.assert_called_once_with(stream_id)
            mock_write_schema.assert_called_once_with(
                stream_id,
                mock_schema,
                schemas.PK_FIELDS[stream_id]
            )

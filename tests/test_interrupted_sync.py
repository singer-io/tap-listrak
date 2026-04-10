"""Integration tests for tap-listrak interrupted sync recovery with mocked data.

tap-listrak uses FULL_TABLE replication on all streams. The sync flow is
hierarchical: sync_lists -> sync_messages -> sync_sub_streams.
An interrupted sync is recoverable by re-running from the top; bookmarks
record the last successful sync timestamp so the next run resumes correctly.
"""
import unittest
from unittest.mock import patch, MagicMock

from .base import ListrakBaseTest

from tap_listrak import streams
from tap_listrak.streams import IDS, BOOK


class ListrakSyncTest(ListrakBaseTest, unittest.TestCase):
    """Verify normal sync behaviour for lists and related streams."""

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_sync_lists_loads_schema_and_writes_lists(
        self, mock_write, mock_request, mock_schema
    ):
        """sync_lists loads the lists schema and writes list records."""
        ctx = self._make_ctx(selected_ids=["lists"])
        mock_request.return_value = [{"ListID": "1", "Name": "Test"}]

        streams.sync_lists(ctx)

        mock_schema.assert_called_once_with(IDS.LISTS)
        mock_write.assert_called_once_with(IDS.LISTS, [{"ListID": "1", "Name": "Test"}])


class ListrakInterruptedSyncTest(ListrakBaseTest, unittest.TestCase):
    """Verify that partial syncs write correct bookmark state."""

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_subscribed_contacts_writes_state_on_completion(
        self, mock_write, mock_request, mock_schema
    ):
        """sync_subscribed_contacts writes state after writing bookmark."""
        ctx = self._make_ctx()
        mock_request.side_effect = [
            [{"ContactID": "C1", "Email": "a@b.com"}],
            [],
        ]

        streams.sync_subscribed_contacts(ctx, [{"ListID": "1"}])

        ctx.set_bookmark.assert_called_with(BOOK.SUBSCRIBED_CONTACTS, ctx.now)
        ctx.write_state.assert_called_once()

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.sync_sub_streams")
    @patch("tap_listrak.streams.sync_message_sends_if_selected")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_messages_writes_state_on_completion(
        self, mock_write, mock_request, mock_sync_sends, mock_sync_subs, mock_schema
    ):
        """sync_messages writes state after updating sub-stream bookmarks."""
        ctx = self._make_ctx(selected_ids=["messages"])
        mock_request.return_value = {
            "ReportListMessageActivityResult": {
                "WSMessageActivity": [
                    {"MsgID": "1", "SendDate": "2026-01-15T00:00:00Z"}
                ]
            }
        }

        streams.sync_messages(ctx, [{"ListID": "1"}])

        ctx.write_state.assert_called_once()

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_empty_lists_response_does_not_crash(
        self, mock_write, mock_request, mock_schema
    ):
        """sync_lists handles None/empty responses gracefully."""
        ctx = self._make_ctx(selected_ids=["lists"])
        mock_request.return_value = None

        streams.sync_lists(ctx)

        # Should still load schema and write empty list
        mock_schema.assert_called_once_with(IDS.LISTS)
        mock_write.assert_called_once_with(IDS.LISTS, [])

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.sync_sub_streams")
    @patch("tap_listrak.streams.sync_message_sends_if_selected")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_messages_no_activity_result_continues(
        self, mock_write, mock_request, mock_sync_sends, mock_sync_subs, mock_schema
    ):
        """sync_messages handles empty ReportListMessageActivityResult gracefully."""
        ctx = self._make_ctx(selected_ids=["messages"])
        mock_request.return_value = {"ReportListMessageActivityResult": None}

        streams.sync_messages(ctx, [{"ListID": "1"}])

        # write_records should not be called for messages (no data)
        mock_write.assert_not_called()
        ctx.write_state.assert_called_once()

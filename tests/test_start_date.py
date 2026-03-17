"""Integration tests for tap-listrak start date functionality with mocked data."""
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

try:
    from .base import ListrakBaseTest
except ImportError:
    from base import ListrakBaseTest

from tap_listrak.context import Context
from tap_listrak import streams
from tap_listrak.streams import IDS, BOOK


class ListrakStartDateTest(ListrakBaseTest, unittest.TestCase):
    """Verify start_date config is accepted and used correctly."""

    def test_context_stores_config(self):
        """Context stores the supplied config including start_date."""
        with patch("tap_listrak.http.zeep") as mock_zeep:
            mock_zeep.Client.return_value = MagicMock()
            ctx = Context(self.get_mock_config(), {})
            self.assertIn("start_date", ctx.config)
            self.assertEqual(ctx.config["start_date"], self.default_start_date)

    def test_update_start_date_bookmark_uses_config_when_no_bookmark(self):
        """update_start_date_bookmark falls back to config start_date when no bookmark exists."""
        with patch("tap_listrak.http.zeep") as mock_zeep:
            mock_zeep.Client.return_value = MagicMock()
            ctx = Context(self.get_mock_config(), {})
            result = ctx.update_start_date_bookmark(BOOK.SUBSCRIBED_CONTACTS)
            # Should parse to a datetime matching the default start date
            self.assertEqual(result.year, 2026)
            self.assertEqual(result.month, 1)
            self.assertEqual(result.day, 1)

    def test_update_start_date_bookmark_uses_state_when_bookmark_exists(self):
        """update_start_date_bookmark uses existing bookmark over config."""
        state = {
            "bookmarks": {
                IDS.SUBSCRIBED_CONTACTS: {
                    "AdditionDate": "2026-01-15T00:00:00Z"
                }
            }
        }
        with patch("tap_listrak.http.zeep") as mock_zeep:
            mock_zeep.Client.return_value = MagicMock()
            ctx = Context(self.get_mock_config(), state)
            result = ctx.update_start_date_bookmark(BOOK.SUBSCRIBED_CONTACTS)
            self.assertEqual(result.day, 15)

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_sync_subscribed_contacts_uses_start_date(
        self, mock_write, mock_request, mock_schema
    ):
        """sync_subscribed_contacts calls update_start_date_bookmark for its bookmark path."""
        ctx = MagicMock(spec=Context)
        ctx.config = self.get_mock_config()
        ctx.now = datetime(2026, 2, 2, 0, 0, 0, tzinfo=timezone.utc)
        ctx.update_start_date_bookmark.return_value = datetime(
            2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc
        )
        ctx.set_bookmark = MagicMock()
        ctx.write_state = MagicMock()
        ctx.client = MagicMock()

        mock_request.side_effect = [[], []]

        streams.sync_subscribed_contacts(ctx, [{"ListID": "1"}])
        ctx.update_start_date_bookmark.assert_called_once_with(BOOK.SUBSCRIBED_CONTACTS)

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_sync_message_sends_skips_old_messages(
        self, mock_write, mock_request, mock_schema
    ):
        """sync_message_sends_if_selected skips messages with SendDate before start_date."""
        ctx = MagicMock(spec=Context)
        ctx.config = self.get_mock_config()
        ctx.now = datetime(2026, 2, 2, 0, 0, 0, tzinfo=timezone.utc)
        # Bookmark start is after old message's SendDate
        ctx.update_start_date_bookmark.return_value = datetime(
            2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc
        )
        ctx.selected_stream_ids = ["message_sends"]
        ctx.client = MagicMock()

        messages = [
            {"MsgID": "1", "SendDate": "2026-01-10T00:00:00Z"},
            {"MsgID": "2", "SendDate": "2026-01-20T00:00:00Z"},
        ]

        # Only message 2 should trigger a request; message 2 page 1 then page 2 (empty)
        mock_request.side_effect = [
            {
                "ReportMessageContactSentResult": {
                    "WSMessageRecipient": [
                        {"RecipientID": "R1", "Email": "a@b.com"}
                    ]
                }
            },
            {"ReportMessageContactSentResult": None},
        ]

        streams.sync_message_sends_if_selected(ctx, messages)

        # request should be called exactly 2 times (for msg 2 only)
        self.assertEqual(mock_request.call_count, 2)

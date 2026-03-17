"""Integration tests for tap-listrak bookmarking with mocked data.

tap-listrak uses FULL_TABLE replication for all streams.
However, several child streams use a start_date-based bookmark to
filter data (subscribed_contacts, message sub-streams, message_sends).
"""
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

try:
    from .base import ListrakBaseTest
except ImportError:
    from base import ListrakBaseTest

from tap_listrak import streams
from tap_listrak.streams import BOOK
from tap_listrak.context import Context


class ListrakBookmarkTest(ListrakBaseTest, unittest.TestCase):
    """Verify bookmark (start-date based) behaviour for streams that use it."""

    def _make_ctx(self, selected_ids=None):
        """Create a mocked Context with common defaults."""
        ctx = MagicMock(spec=Context)
        ctx.config = self.get_mock_config()
        ctx.config["interval_days"] = 365
        ctx.now = datetime(2026, 2, 2, 0, 0, 0, tzinfo=timezone.utc)
        ctx.update_start_date_bookmark.return_value = datetime(
            2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc
        )
        ctx.set_bookmark = MagicMock()
        ctx.write_state = MagicMock()
        ctx.client = MagicMock()
        ctx.client.service = MagicMock()
        ctx.selected_stream_ids = selected_ids or []
        return ctx

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_subscribed_contacts_sets_bookmark_after_sync(
        self, mock_write, mock_request, mock_schema
    ):
        """sync_subscribed_contacts must set its bookmark to ctx.now."""
        ctx = self._make_ctx()
        mock_request.side_effect = [
            [{"ContactID": "C1", "Email": "a@b.com"}],
            [],  # end-of-pages
        ]
        streams.sync_subscribed_contacts(ctx, [{"ListID": "1"}])

        ctx.set_bookmark.assert_called_with(BOOK.SUBSCRIBED_CONTACTS, ctx.now)
        ctx.write_state.assert_called_once()

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_message_sub_stream_updates_bookmark(
        self, mock_write, mock_request, mock_schema
    ):
        """update_sub_stream_bookmarks should set bookmarks for selected sub-streams."""
        ctx = self._make_ctx(selected_ids=["message_clicks", "message_opens"])

        streams.update_sub_stream_bookmarks(ctx)

        # Should be called once for each selected sub-stream
        calls = ctx.set_bookmark.call_args_list
        bookmark_paths = [c[0][0] for c in calls]
        self.assertIn(BOOK.MESSAGE_CLICKS, bookmark_paths)
        self.assertIn(BOOK.MESSAGE_OPENS, bookmark_paths)

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_message_sends_bookmark_uses_max_send_date(
        self, mock_write, mock_request, mock_schema
    ):
        """update_message_sends_bookmark should set bookmark to max SendDate."""
        ctx = self._make_ctx(selected_ids=["message_sends"])

        max_dt = "2026-01-20T14:00:00Z"
        streams.update_message_sends_bookmark(ctx, max_dt)

        ctx.set_bookmark.assert_called_once_with(BOOK.MESSAGE_SENDS, max_dt)

    def test_message_sends_bookmark_not_set_when_not_selected(self):
        """update_message_sends_bookmark should not set bookmark if not selected."""
        ctx = self._make_ctx(selected_ids=[])

        streams.update_message_sends_bookmark(ctx, "2026-01-20T14:00:00Z")
        ctx.set_bookmark.assert_not_called()

    def test_new_max_send_dt_picks_latest(self):
        """new_max_send_dt returns the latest SendDate across messages."""
        messages = [
            {"SendDate": "2026-01-10T00:00:00Z"},
            {"SendDate": "2026-01-20T00:00:00Z"},
        ]
        result = streams.new_max_send_dt(messages, None)
        self.assertEqual(result, "2026-01-20T00:00:00Z")

    def test_new_max_send_dt_keeps_old_if_larger(self):
        """new_max_send_dt keeps old max if it is more recent."""
        messages = [{"SendDate": "2026-01-05T00:00:00Z"}]
        old_max = "2026-01-15T00:00:00Z"
        result = streams.new_max_send_dt(messages, old_max)
        self.assertEqual(result, "2026-01-15T00:00:00Z")

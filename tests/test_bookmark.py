"""Integration tests for tap-listrak bookmarking with mocked data.

tap-listrak uses FULL_TABLE replication for all streams.
However, several child streams use a start_date-based bookmark to
filter data (subscribed_contacts, message sub-streams, message_sends).
"""
import unittest
from unittest.mock import patch, MagicMock

from .base import ListrakBaseTest

from tap_listrak import streams
from tap_listrak.streams import BOOK


class ListrakBookmarkTest(ListrakBaseTest, unittest.TestCase):
    """Verify bookmark (start-date based) behaviour for streams that use it."""

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

        # Both mock responses are consumed: page 1 returns data, page 2 returns [] to stop pagination
        self.assertEqual(mock_request.call_count, 2)
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

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_subscribed_contacts_bookmark_path_is_valid_structure(
        self, mock_write, mock_request, mock_schema
    ):
        """set_bookmark must be called with a two-element [stream_id, date_field] path."""
        ctx = self._make_ctx()
        mock_request.side_effect = [[]]

        streams.sync_subscribed_contacts(ctx, [{"ListID": "1"}])

        args, _ = ctx.set_bookmark.call_args
        bookmark_path = args[0]
        self.assertIsInstance(bookmark_path, list, "Bookmark path must be a list")
        self.assertEqual(len(bookmark_path), 2, "Bookmark path must have exactly [stream_id, date_field]")
        self.assertEqual(bookmark_path[0], "subscribed_contacts")
        self.assertEqual(bookmark_path[1], "AdditionDate")

    def test_message_sub_stream_bookmark_paths_are_valid_structure(self):
        """update_sub_stream_bookmarks must call set_bookmark with valid [stream_id, date_field] paths."""
        expected_paths = {
            "message_clicks": ["message_clicks", "ClickDate"],
            "message_opens": ["message_opens", "OpenDate"],
            "message_reads": ["message_reads", "ReadDate"],
            "message_unsubs": ["message_unsubs", "RemovalDate"],
            "message_bounces": ["message_bounces", "BounceDate"],
        }
        ctx = self._make_ctx(selected_ids=list(expected_paths.keys()))

        streams.update_sub_stream_bookmarks(ctx)

        for call in ctx.set_bookmark.call_args_list:
            path = call[0][0]
            self.assertIsInstance(path, list, f"Bookmark path {path} must be a list")
            self.assertEqual(len(path), 2, f"Bookmark path {path} must have [stream_id, date_field]")
            self.assertIn(path, list(expected_paths.values()),
                          f"Unexpected bookmark path {path}")

    def test_message_sends_bookmark_path_is_valid_structure(self):
        """update_message_sends_bookmark must call set_bookmark with [stream_id, date_field] path."""
        ctx = self._make_ctx(selected_ids=["message_sends"])
        max_dt = "2026-01-20T14:00:00Z"

        streams.update_message_sends_bookmark(ctx, max_dt)

        args, _ = ctx.set_bookmark.call_args
        bookmark_path = args[0]
        self.assertIsInstance(bookmark_path, list, "Bookmark path must be a list")
        self.assertEqual(len(bookmark_path), 2, "Bookmark path must have exactly [stream_id, date_field]")
        self.assertEqual(bookmark_path[0], "message_sends")
        self.assertEqual(bookmark_path[1], "SendDate")

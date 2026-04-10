"""Integration tests for tap-listrak pagination with mocked data."""
import unittest
from unittest.mock import patch, MagicMock

from .base import ListrakBaseTest

from tap_listrak import streams


class ListrakPaginationTest(ListrakBaseTest, unittest.TestCase):
    """Verify page-based pagination for streams that paginate."""

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_subscribed_contacts_multiple_pages(
        self, mock_write, mock_request, mock_schema
    ):
        """sync_subscribed_contacts fetches pages until an empty response."""
        ctx = self._make_ctx()

        page1 = [{"ContactID": f"C{i}", "Email": f"c{i}@e.com"} for i in range(100)]
        page2 = [{"ContactID": "C100", "Email": "c100@e.com"}]

        mock_request.side_effect = [page1, page2, []]

        streams.sync_subscribed_contacts(ctx, [{"ListID": "1"}])

        self.assertEqual(mock_request.call_count, 3)
        self.assertEqual(mock_write.call_count, 2)

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_subscribed_contacts_single_page(
        self, mock_write, mock_request, mock_schema
    ):
        """sync_subscribed_contacts with one page of data stops after empty second page."""
        ctx = self._make_ctx()

        mock_request.side_effect = [
            [{"ContactID": "C1", "Email": "c1@e.com"}],
            [],
        ]

        streams.sync_subscribed_contacts(ctx, [{"ListID": "1"}])

        self.assertEqual(mock_request.call_count, 2)
        self.assertEqual(mock_write.call_count, 1)

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_message_sub_stream_paginates_per_message(
        self, mock_write, mock_request, mock_schema
    ):
        """sync_message_sub_stream loops pages for each message."""
        ctx = self._make_ctx()
        sub_stream = streams.MESSAGE_SUB_STREAMS[0]

        messages = [
            {"MsgID": "1", "SendDate": "2026-01-10T00:00:00Z"},
            {"MsgID": "2", "SendDate": "2026-01-20T00:00:00Z"},
        ]

        mock_request.side_effect = [
            # msg 1 page 1, msg 1 page 2 (empty)
            [{"ClickID": "1", "ClickDate": "2026-01-11T00:00:00Z"}],
            [],
            # msg 2 page 1, msg 2 page 2 (empty)
            [{"ClickID": "2", "ClickDate": "2026-01-21T00:00:00Z"}],
            [],
        ]

        streams.sync_message_sub_stream(ctx, messages, sub_stream)

        self.assertEqual(mock_request.call_count, 4)
        self.assertEqual(mock_write.call_count, 2)

    @patch("tap_listrak.schemas.load_and_write_schema")
    @patch("tap_listrak.streams.request")
    @patch("tap_listrak.streams.write_records")
    def test_message_sends_paginates_per_message(
        self, mock_write, mock_request, mock_schema
    ):
        """sync_message_sends_if_selected pages through each message's recipients."""
        ctx = self._make_ctx(selected_ids=["message_sends"])

        messages = [{"MsgID": "1", "SendDate": "2026-01-15T00:00:00Z"}]

        mock_request.side_effect = [
            {
                "ReportMessageContactSentResult": {
                    "WSMessageRecipient": [
                        {"RecipientID": "R1", "Email": "a@b.com"},
                        {"RecipientID": "R2", "Email": "c@d.com"},
                    ]
                }
            },
            {"ReportMessageContactSentResult": None},
        ]

        streams.sync_message_sends_if_selected(ctx, messages)

        self.assertEqual(mock_request.call_count, 2)
        self.assertEqual(mock_write.call_count, 1)

    def test_gen_pages_yields_incrementing_integers(self):
        """gen_pages yields 1, 2, 3, ... indefinitely."""
        gen = streams.gen_pages()
        pages = [next(gen) for _ in range(5)]
        self.assertEqual(pages, [1, 2, 3, 4, 5])

    def test_gen_intervals_covers_date_range(self):
        """gen_intervals generates contiguous non-overlapping date windows."""
        ctx = self._make_ctx()
        intervals = list(streams.gen_intervals(ctx, "2025-01-01T00:00:00Z"))
        self.assertGreater(len(intervals), 0)
        # Last interval should reach ctx.now
        self.assertEqual(intervals[-1][1], ctx.now)
        for i in range(1, len(intervals)):
            self.assertEqual(intervals[i][0], intervals[i - 1][1])

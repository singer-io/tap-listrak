import unittest
import pendulum
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from tap_listrak import streams
from tap_listrak.context import Context

class TestSyncFunctions(unittest.TestCase):

    def setUp(self):
        self.ctx = MagicMock(spec=Context)
        self.ctx.update_start_date_bookmark.return_value = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.ctx.now = datetime(2026, 2, 2, 0, 0, 0, tzinfo=timezone.utc)
        self.ctx.set_bookmark = MagicMock()
        self.ctx.write_state = MagicMock()
        self.ctx.client = MagicMock()  # Mock the client attribute
        self.ctx.client.service = MagicMock()  # Mock the service attribute
        self.ctx.config = {'start_date': '2026-01-01T00:00:00Z', 'interval_days': 365}
        self.ctx.selected_stream_ids = []

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_subscribed_contacts_with_lists(self, mock_write_records, mock_request, mock_load_schema):
        mock_request.side_effect = [
            [{'ContactID': '123', 'Email': 'test@example.com'}],  # Mock response for list 1, page 1
            [],  # Mock response for list 1, page 2 (end of data)
            [{'ContactID': '456', 'Email': 'test2@example.com'}],  # Mock response for list 2, page 1
            []  # Mock response for list 2, page 2 (end of data)
        ]

        streams.sync_subscribed_contacts(self.ctx, [{'ListID': '1'}, {'ListID': '2'}])

        # Assert that schema was loaded
        mock_load_schema.assert_called_once_with(streams.IDS.SUBSCRIBED_CONTACTS)

        # Assert that the request was called for subscribed contacts
        self.assertEqual(mock_request.call_count, 4)

        # Assert that write_records was called with the correct data for both lists
        self.assertEqual(mock_write_records.call_count, 2)

        # Verify first list contacts
        mock_write_records.assert_any_call(
            streams.IDS.SUBSCRIBED_CONTACTS,
            [{'ContactID': '123', 'Email': 'test@example.com', 'ListID': '1'}]
        )

        # Verify second list contacts
        mock_write_records.assert_any_call(
            streams.IDS.SUBSCRIBED_CONTACTS,
            [{'ContactID': '456', 'Email': 'test2@example.com', 'ListID': '2'}]
        )

        # Assert that bookmarks and state were updated
        self.ctx.set_bookmark.assert_called_with(streams.BOOK.SUBSCRIBED_CONTACTS, self.ctx.now)
        self.ctx.write_state.assert_called_once()


    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_lists(self, mock_write_records, mock_request, mock_load_schema):
        self.ctx.selected_stream_ids = ['lists']

        mock_request.return_value = [{'ListID': '1', 'Name': 'Test List'}]

        streams.sync_lists(self.ctx)

        # Assert that schema was loaded
        mock_load_schema.assert_called_once_with(streams.IDS.LISTS)

        # Assert that the request was called for lists
        mock_request.assert_called_once_with(
            streams.IDS.LISTS, self.ctx.client.service.GetContactListCollection
        )

        # Assert that write_records was called with the correct data
        mock_write_records.assert_called_with(
            streams.IDS.LISTS,
            [{'ListID': '1', 'Name': 'Test List'}]
        )

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_messages_with_list(self, mock_write_records, mock_request, mock_load_schema):
        self.ctx.selected_stream_ids = ['messages']

        mock_request.side_effect = [
            {
                'ReportListMessageActivityResult': {
                    'WSMessageActivity': [
                        {'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}
                    ]
                }
            },  # Response with messages
        ]

        streams.sync_messages(self.ctx, [{'ListID': '1', 'Name': 'Test List'}])

        # Assert that schema was loaded
        mock_load_schema.assert_called_once_with(streams.IDS.MESSAGES)

        # Assert that the request was called for messages
        self.assertEqual(mock_request.call_count, 1)

        # Assert that write_records was called with the correct data
        mock_write_records.assert_called_with(
            streams.IDS.MESSAGES,
            [{'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}]
        )


    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_message_sub_stream(self, mock_write_records, mock_request, mock_load_schema):
        self.ctx.selected_stream_ids = ['message_clicks']

        messages = [
            {'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'},
            {'MsgID': '2', 'SendDate': '2026-01-20T00:00:00Z'}
        ]

        sub_stream = streams.MESSAGE_SUB_STREAMS[0]  # MESSAGE_CLICKS

        mock_request.side_effect = [
            [{'ClickID': '1', 'ClickDate': '2026-01-16T00:00:00Z'}],  # Msg 1, Page 1
            [],  # Msg 1, Page 2 (end)
            [{'ClickID': '2', 'ClickDate': '2026-01-21T00:00:00Z'}],  # Msg 2, Page 1
            []  # Msg 2, Page 2 (end)
        ]

        streams.sync_message_sub_stream(self.ctx, messages, sub_stream)

        # Assert that schema was loaded
        mock_load_schema.assert_called_once_with(streams.IDS.MESSAGE_CLICKS)

        # Assert that requests were made for both messages
        self.assertEqual(mock_request.call_count, 4)

        # Assert that write_records was called with the correct data
        self.assertEqual(mock_write_records.call_count, 2)

        mock_write_records.assert_any_call(
            streams.IDS.MESSAGE_CLICKS,
            [{'ClickID': '1', 'ClickDate': '2026-01-16T00:00:00Z', 'MsgID': '1'}]
        )

        mock_write_records.assert_any_call(
            streams.IDS.MESSAGE_CLICKS,
            [{'ClickID': '2', 'ClickDate': '2026-01-21T00:00:00Z', 'MsgID': '2'}]
        )

    @patch('tap_listrak.streams.sync_message_sub_stream')
    def test_sync_sub_streams(self, mock_sync_message_sub_stream):
        self.ctx.selected_stream_ids = ['message_clicks', 'message_opens']

        messages = [{'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}]

        streams.sync_sub_streams(self.ctx, messages)

        # Assert that sync_message_sub_stream was called for each selected sub stream
        self.assertEqual(mock_sync_message_sub_stream.call_count, 2)

        # Verify it was called with the correct parameters
        calls = mock_sync_message_sub_stream.call_args_list
        stream_ids_called = [call[0][2].tap_stream_id for call in calls]
        self.assertIn('message_clicks', stream_ids_called)
        self.assertIn('message_opens', stream_ids_called)

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_message_sends_if_selected_with_selection(self, mock_write_records, mock_request, mock_load_schema):
        self.ctx.selected_stream_ids = ['message_sends']

        messages = [{'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}]

        mock_request.side_effect = [
            {
                'ReportMessageContactSentResult': {
                    'WSMessageRecipient': [
                        {'RecipientID': '1', 'Email': 'user@example.com'}
                    ]
                }
            },  # Page 1
            {'ReportMessageContactSentResult': None}  # Page 2 (end)
        ]

        streams.sync_message_sends_if_selected(self.ctx, messages)

        # Assert that schema was loaded
        mock_load_schema.assert_called_once_with(streams.IDS.MESSAGE_SENDS)

        # Assert that requests were made
        self.assertEqual(mock_request.call_count, 2)

        # Assert that write_records was called with the correct data
        mock_write_records.assert_called_once_with(
            streams.IDS.MESSAGE_SENDS,
            [{'RecipientID': '1', 'Email': 'user@example.com', 'MsgID': '1'}]
        )

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_message_sends_if_selected_without_selection(self, mock_write_records, mock_request, mock_load_schema):
        self.ctx.selected_stream_ids = []  # MESSAGE_SENDS not selected

        messages = [{'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}]

        streams.sync_message_sends_if_selected(self.ctx, messages)

        # Assert that no requests were made
        mock_request.assert_not_called()
        mock_write_records.assert_not_called()
        mock_load_schema.assert_not_called()

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.sync_subscribed_contacts')
    @patch('tap_listrak.streams.sync_messages')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_lists_with_nested_streams(self, mock_write_records, mock_request,
                                            mock_sync_messages, mock_sync_subscribed_contacts, mock_load_schema):
        self.ctx.selected_stream_ids = ['lists', 'messages', 'subscribed_contacts']

        mock_request.return_value = [
            {'ListID': '1', 'Name': 'Test List 1'},
            {'ListID': '2', 'Name': 'Test List 2'}
        ]

        streams.sync_lists(self.ctx)

        # Assert that schema was loaded for lists
        mock_load_schema.assert_called_once_with(streams.IDS.LISTS)

        # Assert that lists were fetched and written
        mock_request.assert_called_once_with(
            streams.IDS.LISTS, self.ctx.client.service.GetContactListCollection
        )

        mock_write_records.assert_called_once_with(
            streams.IDS.LISTS,
            [
                {'ListID': '1', 'Name': 'Test List 1'},
                {'ListID': '2', 'Name': 'Test List 2'}
            ]
        )

        # Assert that nested sync functions were called with the lists
        mock_sync_messages.assert_called_once_with(
            self.ctx,
            [
                {'ListID': '1', 'Name': 'Test List 1'},
                {'ListID': '2', 'Name': 'Test List 2'}
            ]
        )

        mock_sync_subscribed_contacts.assert_called_once_with(
            self.ctx,
            [
                {'ListID': '1', 'Name': 'Test List 1'},
                {'ListID': '2', 'Name': 'Test List 2'}
            ]
        )

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_lists_without_nested_streams(self, mock_write_records, mock_request, mock_load_schema):
        self.ctx.selected_stream_ids = ['lists']  # Only lists, no nested streams

        mock_request.return_value = [{'ListID': '1', 'Name': 'Test List'}]

        streams.sync_lists(self.ctx)

        # Assert that schema was loaded
        mock_load_schema.assert_called_once_with(streams.IDS.LISTS)

        # Assert that lists were fetched and written
        mock_request.assert_called_once()
        mock_write_records.assert_called_once()

    # Tests for child stream schema loading and parent dependencies

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_subscribed_contacts_loads_schema(self, mock_write_records, mock_request, mock_load_schema):
        """Test that sync_subscribed_contacts loads its schema."""
        mock_request.side_effect = [
            [{'ContactID': '123', 'Email': 'test@example.com'}],
            []
        ]

        streams.sync_subscribed_contacts(self.ctx, [{'ListID': '1'}])

        # Assert that schema was loaded for subscribed_contacts
        mock_load_schema.assert_called_once_with(streams.IDS.SUBSCRIBED_CONTACTS)

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.sync_sub_streams')
    @patch('tap_listrak.streams.sync_message_sends_if_selected')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_messages_loads_schema(self, mock_write_records, mock_request,
                                        mock_sync_sends, mock_sync_subs, mock_load_schema):
        """Test that sync_messages loads its schema."""
        self.ctx.selected_stream_ids = ['messages']

        mock_request.return_value = {
            'ReportListMessageActivityResult': {
                'WSMessageActivity': [
                    {'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}
                ]
            }
        }

        streams.sync_messages(self.ctx, [{'ListID': '1', 'Name': 'Test List'}])

        # Assert that schema was loaded for messages
        mock_load_schema.assert_called_once_with(streams.IDS.MESSAGES)

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_message_sub_stream_loads_schema(self, mock_write_records, mock_request, mock_load_schema):
        """Test that sync_message_sub_stream loads schema for the sub stream."""
        messages = [{'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}]
        sub_stream = streams.MESSAGE_SUB_STREAMS[0]  # MESSAGE_CLICKS

        mock_request.side_effect = [
            [{'ClickID': '1', 'ClickDate': '2026-01-16T00:00:00Z'}],
            []
        ]

        streams.sync_message_sub_stream(self.ctx, messages, sub_stream)

        # Assert that schema was loaded for message_clicks
        mock_load_schema.assert_called_once_with(streams.IDS.MESSAGE_CLICKS)

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.sync_message_sub_stream')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_messages_child_stream_triggers_parent_syncs(self, mock_write_records, mock_request,
                                                          mock_sync_sub, mock_load_schema):
        """Test that message child streams are synced through their parent messages stream."""
        self.ctx.selected_stream_ids = ['message_clicks']

        # Mock the request to return messages data
        mock_request.return_value = {
            'ReportListMessageActivityResult': {
                'WSMessageActivity': [
                    {'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}
                ]
            }
        }

        lists = [{'ListID': '1', 'Name': 'Test List'}]

        # Call sync_messages which should trigger sync_sub_streams for child streams
        streams.sync_messages(self.ctx, lists)

        # Verify that sync_message_sub_stream was called for the selected sub-stream
        mock_sync_sub.assert_called_once()

        # Verify messages schema was loaded
        mock_load_schema.assert_called_once_with(streams.IDS.MESSAGES)

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.sync_messages')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_lists_calls_sync_messages_when_messages_selected(self, mock_write_records,
                                                                    mock_request, mock_sync_messages,
                                                                    mock_load_schema):
        """Test that sync_lists calls sync_messages when messages stream is selected."""
        self.ctx.selected_stream_ids = ['lists', 'messages']

        mock_request.return_value = [
            {'ListID': '1', 'Name': 'Test List 1'},
            {'ListID': '2', 'Name': 'Test List 2'}
        ]

        streams.sync_lists(self.ctx)

        # Assert sync_messages was called as a child of lists
        mock_sync_messages.assert_called_once_with(
            self.ctx,
            [
                {'ListID': '1', 'Name': 'Test List 1'},
                {'ListID': '2', 'Name': 'Test List 2'}
            ]
        )

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.sync_subscribed_contacts')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_lists_calls_sync_subscribed_contacts_when_selected(self, mock_write_records,
                                                                      mock_request, mock_sync_contacts,
                                                                      mock_load_schema):
        """Test that sync_lists calls sync_subscribed_contacts when that stream is selected."""
        self.ctx.selected_stream_ids = ['lists', 'subscribed_contacts']

        mock_request.return_value = [
            {'ListID': '1', 'Name': 'Test List'}
        ]

        streams.sync_lists(self.ctx)

        # Assert sync_subscribed_contacts was called as a child of lists
        mock_sync_contacts.assert_called_once_with(
            self.ctx,
            [{'ListID': '1', 'Name': 'Test List'}]
        )

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.sync_sub_streams')
    @patch('tap_listrak.streams.sync_message_sends_if_selected')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_messages_calls_child_sync_functions(self, mock_write_records, mock_request,
                                                       mock_sync_sends, mock_sync_subs,
                                                       mock_load_schema):
        """Test that sync_messages calls child sync functions for sub-streams."""
        self.ctx.selected_stream_ids = ['messages', 'message_clicks']

        mock_request.return_value = {
            'ReportListMessageActivityResult': {
                'WSMessageActivity': [
                    {'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}
                ]
            }
        }

        lists = [{'ListID': '1', 'Name': 'Test List'}]
        streams.sync_messages(self.ctx, lists)

        # Assert that child sync functions were called
        messages = [{'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}]
        mock_sync_subs.assert_called_once_with(self.ctx, messages)
        mock_sync_sends.assert_called_once_with(self.ctx, messages)

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_multiple_message_sub_streams_load_schemas(self, mock_write_records, mock_request, mock_load_schema):
        """Test that multiple message sub-streams each load their schemas."""
        messages = [{'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}]

        # Test message_opens sub-stream
        sub_stream = streams.MESSAGE_SUB_STREAMS[1]  # MESSAGE_OPENS
        mock_request.side_effect = [
            [{'OpenID': '1', 'OpenDate': '2026-01-16T00:00:00Z'}],
            []
        ]

        streams.sync_message_sub_stream(self.ctx, messages, sub_stream)

        # Assert schema was loaded for message_opens
        mock_load_schema.assert_called_with(streams.IDS.MESSAGE_OPENS)

class TestGenIntervals(unittest.TestCase):
    """
    Regression tests for the offset-naive vs offset-aware datetime comparison bug.

    Previously, Context.now was set with datetime.utcnow() (naive), while
    pendulum.parse() returns a timezone-aware datetime. Comparing them raised:
        TypeError: can't compare offset-naive and offset-aware datetimes
    The fix was to use pendulum.now("UTC") so that ctx.now is always
    timezone-aware and comparable to pendulum-parsed datetimes.
    """

    def _make_ctx(self, now_dt):
        ctx = MagicMock()
        ctx.now = now_dt
        ctx.config = {}
        return ctx

    def test_gen_intervals_raises_with_naive_now(self):
        """Naive ctx.now compared to timezone-aware start_dt should raise TypeError."""
        naive_now = datetime(2026, 3, 17, 0, 0, 0)  # no tzinfo
        ctx = self._make_ctx(naive_now)
        start_str = "2026-01-01T00:00:00Z"
        with self.assertRaises(TypeError):
            list(streams.gen_intervals(ctx, start_str))

    def test_gen_intervals_works_with_aware_now(self):
        """Timezone-aware ctx.now (pendulum) should not raise and should yield intervals."""
        aware_now = pendulum.now("UTC")
        ctx = self._make_ctx(aware_now)
        start_str = "2026-01-01T00:00:00Z"
        intervals = list(streams.gen_intervals(ctx, start_str))
        self.assertGreater(len(intervals), 0)
        for begin_dt, end_dt in intervals:
            self.assertIsNotNone(begin_dt.tzinfo)
            self.assertIsNotNone(end_dt.tzinfo)
            self.assertLess(begin_dt, end_dt)

    def test_context_now_is_timezone_aware(self):
        """Context.now must be timezone-aware after the pendulum.now('UTC') fix."""
        config = {"start_date": "2026-01-01T00:00:00Z", "username": "u", "password": "p"}
        state = {}
        with patch("tap_listrak.context.get_client", return_value=MagicMock()):
            ctx = Context(config, state)
        self.assertIsNotNone(ctx.now.tzinfo,
            "ctx.now must be timezone-aware to avoid comparison errors with pendulum-parsed datetimes")


if __name__ == '__main__':
    unittest.main()

import unittest
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone
from tap_listrak import streams
from tap_listrak.context import Context
from tap_listrak import sync

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

    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_subscribed_contacts_with_lists(self, mock_write_records, mock_request):
        mock_request.side_effect = [
            [{'ContactID': '123', 'Email': 'test@example.com'}],  # Mock response for list 1, page 1
            [],  # Mock response for list 1, page 2 (end of data)
            [{'ContactID': '456', 'Email': 'test2@example.com'}],  # Mock response for list 2, page 1
            []  # Mock response for list 2, page 2 (end of data)
        ]

        streams._sync_subscribed_contacts(self.ctx, [{'ListID': '1'}, {'ListID': '2'}])

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


    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_lists(self, mock_write_records, mock_request):
        self.ctx.selected_stream_ids = ['lists']

        mock_request.return_value = [{'ListID': '1', 'Name': 'Test List'}]

        streams.sync_lists(self.ctx)

        # Assert that the request was called for lists
        mock_request.assert_called_once_with(
            streams.IDS.LISTS, self.ctx.client.service.GetContactListCollection
        )

        # Assert that write_records was called with the correct data
        mock_write_records.assert_called_with(
            streams.IDS.LISTS,
            [{'ListID': '1', 'Name': 'Test List'}]
        )

    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_messages_with_list(self, mock_write_records, mock_request):
        self.ctx.selected_stream_ids = ['messages']

        mock_request.side_effect = [
            {
                'ReportListMessageActivityResult': {
                    'WSMessageActivity': [
                        {'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}
                    ]
                }
            },  # Mock response for messages
        ]

        streams._sync_messages(self.ctx, [{'ListID': '1', 'Name': 'Test List'}])

        # Assert that the request was called for messages
        self.assertEqual(mock_request.call_count, 1)

        # Assert that write_records was called with the correct data
        mock_write_records.assert_called_with(
            streams.IDS.MESSAGES,
            [{'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}]
        )
        self.ctx.selected_stream_ids = ['messages']

        mock_request.side_effect = [
            {
                'ReportListMessageActivityResult': {
                    'WSMessageActivity': [
                        {'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}
                    ]
                }
            },  # Mock response for messages
        ]

        streams._sync_messages(self.ctx, [{'ListID': '1', 'Name': 'Test List'}])

        # Assert that the request was called twice: once for lists, once for messages
        self.assertEqual(mock_request.call_count, 2)

    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_message_sub_stream(self, mock_write_records, mock_request):
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
        
        streams._sync_message_sub_stream(self.ctx, messages, sub_stream)
        
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

    @patch('tap_listrak.streams._sync_message_sub_stream')
    def test_sync_sub_streams(self, mock_sync_message_sub_stream):
        self.ctx.selected_stream_ids = ['message_clicks', 'message_opens']
        
        messages = [{'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}]
        
        streams._sync_sub_streams(self.ctx, messages)
        
        # Assert that _sync_message_sub_stream was called for each selected sub stream
        self.assertEqual(mock_sync_message_sub_stream.call_count, 2)

    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_message_sends_if_selected_with_selection(self, mock_write_records, mock_request):
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
        
        streams._sync_message_sends_if_selected(self.ctx, messages)
        
        # Assert that requests were made
        self.assertEqual(mock_request.call_count, 2)
        
        # Assert that write_records was called with the correct data
        mock_write_records.assert_called_once_with(
            streams.IDS.MESSAGE_SENDS,
            [{'RecipientID': '1', 'Email': 'user@example.com', 'MsgID': '1'}]
        )

    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_message_sends_if_selected_without_selection(self, mock_write_records, mock_request):
        self.ctx.selected_stream_ids = []  # MESSAGE_SENDS not selected
        
        messages = [{'MsgID': '1', 'SendDate': '2026-01-15T00:00:00Z'}]
        
        streams._sync_message_sends_if_selected(self.ctx, messages)
        
        # Assert that no requests were made
        mock_request.assert_not_called()
        mock_write_records.assert_not_called()

    @patch('tap_listrak.streams._sync_subscribed_contacts')
    @patch('tap_listrak.streams._sync_messages')
    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_lists_with_nested_streams(self, mock_write_records, mock_request, 
                                            mock_sync_messages, mock_sync_subscribed_contacts):
        self.ctx.selected_stream_ids = ['lists', 'messages', 'subscribed_contacts']
        
        mock_request.return_value = [
            {'ListID': '1', 'Name': 'Test List 1'},
            {'ListID': '2', 'Name': 'Test List 2'}
        ]
        
        streams.sync_lists(self.ctx)
        
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

    @patch('tap_listrak.streams.request')
    @patch('tap_listrak.streams.write_records')
    def test_sync_lists_without_nested_streams(self, mock_write_records, mock_request):
        self.ctx.selected_stream_ids = ['lists']  # Only lists, no nested streams
        
        mock_request.return_value = [{'ListID': '1', 'Name': 'Test List'}]
        
        streams.sync_lists(self.ctx)
        
        # Assert that lists were fetched and written
        mock_request.assert_called_once()
        mock_write_records.assert_called_once()

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.sync_lists')
    def test_main_sync_with_lists_dependent_streams(self, mock_sync_lists, mock_load_schema):
        self.ctx.selected_stream_ids = ['lists', 'messages']
        
        sync(self.ctx)

        # Assert that schema was loaded for lists and messages (order-independent)
        self.assertCountEqual(mock_load_schema.call_args_list, [call('lists'), call('messages')])
        
        # Assert that sync_lists was called
        mock_sync_lists.assert_called_once_with(self.ctx)
        
        # Assert that state was written
        self.ctx.write_state.assert_called_once()

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.sync_lists')
    def test_main_sync_with_message_substreams(self, mock_sync_lists, mock_load_schema):
        self.ctx.selected_stream_ids = ['message_clicks', 'message_opens']
        
        sync(self.ctx)
        
        # Assert that schema was loaded for all parent streams (order-independent)
        self.assertCountEqual(mock_load_schema.call_args_list, [call('lists'), call('messages'), call('message_clicks'), call('message_opens')])
        
        # Assert that sync_lists was called (handles all list-dependent streams)
        mock_sync_lists.assert_called_once_with(self.ctx)
        
        # Assert that state was written
        self.ctx.write_state.assert_called_once()

    @patch('tap_listrak.schemas.load_and_write_schema')
    @patch('tap_listrak.streams.sync_lists')
    def test_main_sync_with_subscribed_contacts(self, mock_sync_lists, mock_load_schema):
        self.ctx.selected_stream_ids = ['subscribed_contacts']
        
        sync(self.ctx)
        
        # Assert that schema was loaded for lists and subscribed_contacts (order-independent)
        self.assertCountEqual(mock_load_schema.call_args_list, [call('lists'), call('subscribed_contacts')])
        
        # Assert that sync_lists was called
        mock_sync_lists.assert_called_once_with(self.ctx)
        
        # Assert that state was written
        self.ctx.write_state.assert_called_once()

if __name__ == '__main__':
    unittest.main()

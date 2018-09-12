# tap-listrak

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Listrak Email RESTful API](https://api.listrak.com/email)
- Extracts the following resources:
    - [Lists](https://api.listrak.com/email#operation/List_GetListCollection)
    - [Campaigns](https://api.listrak.com/email#operation/Campaign_GetCampaignCollection)
    - [Contacts](https://api.listrak.com/email#tag/Contact)
    - [Messages](https://api.listrak.com/email#tag/Message)
    - [Message Activity](https://api.listrak.com/email#operation/Message_GetMessageResource)
    - [Message Links](https://api.listrak.com/email#operation/MessageLink_GetMessageLinkCollection)
    - [Message Link Clickers](https://api.listrak.com/email#operation/MessageLinkClicker_GetMessageLinkClickerCollection)
    - [Conversations](https://api.listrak.com/email#operation/Conversation_GetConversationCollection)
    - [Conversation Messages](https://api.listrak.com/email#tag/ConversationMessage)
    - [Conversation Message Activity](https://api.listrak.com/email#tag/ConversationMessageActivity)
    - [Transactional Messages](https://api.listrak.com/email#operation/TransactionalMessage_GetTransactionalMessageCollection)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick Start

1. Install

	```sh
	pip install tap-listrak
	```

2. Create the config file

   You must create a JSON configuration file that looks like this:

   ```json
   {
     "start_date": "2010-01-01",
     "client_id": "your-listrak-client-id",
     "client_secret": "your-listrak-client-secret"
   }
   ```

   The `start_date` is the date at which the tap will begin pulling data. The
   Listrak API uses a form of OAuth2 Client Credentials Authentication, meaning you must create these credentials in Listrak and provide them.

3. Run the Tap in Discovery Mode

	```sh
   tap-listrak -c config.json --discover
   ```

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#discover-mode-and-connection-checks).

4. Run the Tap in Sync Mode

	```sh
   tap-listrak -c config.json --catalog catalog-file.json
   ```

---

Copyright &copy; 2018 Stitch
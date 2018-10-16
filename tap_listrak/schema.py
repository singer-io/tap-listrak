import os
import json

SCHEMAS = None

PKS = {
    'lists': ['listId'],
    'campaigns': ['campaignId'],
    'contacts': ['listId', 'emailAddress'],
    'messages': ['messageId'],
    'message_links': ['linkId'],
    'message_link_clickers': ['linkId', 'emailAddress', 'clickDate'],
    'message_activity': [],
    'conversations': ['conversationId'],
    'conversation_messages': ['messageId'],
    'conversation_message_activity': [],
    'transactional_messages': ['transactionalMessageId']
}

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():
    global SCHEMAS

    if SCHEMAS:
        return SCHEMAS

    schemas_path = get_abs_path('schemas')
    schema_filenames = [f for f in os.listdir(schemas_path)
                        if os.path.isfile(os.path.join(schemas_path, f))]

    SCHEMAS = {}

    for filename in schema_filenames:
        stream_name = filename.replace('.json', '')
        with open(os.path.join(schemas_path, filename)) as file:
            SCHEMAS[stream_name] = json.load(file)

    return SCHEMAS

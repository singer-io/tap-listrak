import json
import os


class ListrakBaseTest:
    """Base test case for tap-listrak integration tests with mocked data."""

    PRIMARY_KEYS = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS = "replication_keys"
    OBEYS_START_DATE = "obeys_start_date"
    PARENT = "parent"

    default_start_date = "2026-01-01T00:00:00Z"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "lists": {
                cls.PRIMARY_KEYS: {"ListID"},
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
            },
            "messages": {
                cls.PRIMARY_KEYS: {"MsgID"},
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.PARENT: "lists",
            },
            "subscribed_contacts": {
                cls.PRIMARY_KEYS: {"ListID", "ContactID"},
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: True,
                cls.PARENT: "lists",
            },
            "message_clicks": {
                cls.PRIMARY_KEYS: {"MsgID", "EmailAddress"},
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: True,
                cls.PARENT: "messages",
            },
            "message_opens": {
                cls.PRIMARY_KEYS: {"MsgID", "EmailAddress"},
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: True,
                cls.PARENT: "messages",
            },
            "message_reads": {
                cls.PRIMARY_KEYS: {"MsgID", "EmailAddress"},
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: True,
                cls.PARENT: "messages",
            },
            "message_sends": {
                cls.PRIMARY_KEYS: {"MsgID", "EmailAddress"},
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: True,
                cls.PARENT: "messages",
            },
            "message_unsubs": {
                cls.PRIMARY_KEYS: {"MsgID", "EmailAddress"},
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: True,
                cls.PARENT: "messages",
            },
            "message_bounces": {
                cls.PRIMARY_KEYS: {"MsgID", "EmailAddress"},
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: True,
                cls.PARENT: "messages",
            },
        }

    def setUp(self):
        """Set up test fixtures."""
        self.config = self.get_mock_config()
        self.state = {}

    def tearDown(self):
        """Clean up after tests."""
        pass

    @staticmethod
    def get_mock_config():
        """Return mock configuration."""
        return {
            "username": "mock_test_user",
            "password": "mock_test_pass",
            "start_date": "2026-01-01T00:00:00Z",
        }

    @staticmethod
    def get_mock_state():
        """Return initial mock state."""
        return {}

    @staticmethod
    def _schema_path(stream_name):
        base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        return os.path.join(base_dir, "tap_listrak", "schemas", f"{stream_name}.json")

    @classmethod
    def _load_schema(cls, stream_name):
        with open(cls._schema_path(stream_name), "r", encoding="utf-8") as schema_file:
            return json.load(schema_file)

    @staticmethod
    def _schema_type(schema):
        """Return concrete type when schema allows null union types."""
        schema_type = schema.get("type", "object")
        if isinstance(schema_type, list):
            non_null = [item for item in schema_type if item != "null"]
            return non_null[0] if non_null else "null"
        return schema_type

    @staticmethod
    def _generate_value(schema, date_value="2026-01-15T00:00:00Z"):
        """Generate one valid mock value for a JSON-schema fragment."""
        if "enum" in schema and schema["enum"]:
            return schema["enum"][0]

        schema_type = ListrakBaseTest._schema_type(schema)
        if schema_type == "object":
            properties = schema.get("properties", {})
            return {
                key: ListrakBaseTest._generate_value(value, date_value=date_value)
                for key, value in properties.items()
            }
        if schema_type == "array":
            return [
                ListrakBaseTest._generate_value(
                    schema.get("items", {"type": "string"}),
                    date_value=date_value,
                )
            ]
        if schema_type == "string":
            fmt = schema.get("format")
            return (
                date_value
                if fmt == "date-time"
                else "mock@example.com"
                if fmt == "email"
                else "mock"
            )
        return {"integer": 1, "number": 1.0, "boolean": True}.get(schema_type)

    @classmethod
    def _generate_stream_record(cls, stream_name, date_value="2026-01-15T00:00:00Z"):
        """Generate one schema-valid record for a stream."""
        return cls._generate_value(cls._load_schema(stream_name), date_value=date_value)

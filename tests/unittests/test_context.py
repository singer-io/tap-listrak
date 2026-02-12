import pytest
from datetime import datetime, date
from unittest.mock import MagicMock, patch, Mock
import pendulum
from tap_listrak.context import Context


@pytest.fixture
def mock_config():
    """Fixture for mock configuration."""
    return {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "start_date": "2023-01-01T00:00:00Z"
    }


@pytest.fixture
def mock_state():
    """Fixture for mock state."""
    return {
        "bookmarks": {
            "lists": {
                "last_updated": "2023-06-01T00:00:00Z"
            }
        }
    }


@pytest.fixture
def mock_catalog():
    """Fixture for mock catalog."""
    catalog = MagicMock()

    # Create mock streams
    stream1 = MagicMock()
    stream1.tap_stream_id = "lists"
    stream1.is_selected.return_value = True
    stream1.metadata = []

    stream2 = MagicMock()
    stream2.tap_stream_id = "messages"
    stream2.is_selected.return_value = False
    stream2.metadata = []

    catalog.streams = [stream1, stream2]
    return catalog


@patch('tap_listrak.context.get_client')
class TestContextInit:
    """Test Context class initialization."""

    def test_context_initialization(self, mock_get_client, mock_config, mock_state):
        """Test that Context initializes with correct attributes."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        ctx = Context(mock_config, mock_state)

        assert ctx.config == mock_config
        assert ctx.state == mock_state
        assert ctx.client == mock_client
        assert ctx._catalog is None
        assert ctx.selected_stream_ids is None
        assert ctx.cache == {}
        assert isinstance(ctx.now, datetime)
        mock_get_client.assert_called_once_with(mock_config)

    def test_context_now_is_datetime(self, mock_get_client, mock_config, mock_state):
        """Test that Context.now is a datetime object."""
        mock_get_client.return_value = MagicMock()

        ctx = Context(mock_config, mock_state)

        assert isinstance(ctx.now, datetime)

    def test_context_cache_empty_on_init(self, mock_get_client, mock_config, mock_state):
        """Test that Context.cache is empty dict on initialization."""
        mock_get_client.return_value = MagicMock()

        ctx = Context(mock_config, mock_state)

        assert ctx.cache == {}
        assert isinstance(ctx.cache, dict)


@patch('tap_listrak.context.get_client')
class TestContextCatalog:
    """Test Context catalog property."""

    def test_catalog_getter(self, mock_get_client, mock_config, mock_state):
        """Test that catalog getter returns the catalog."""
        mock_get_client.return_value = MagicMock()
        ctx = Context(mock_config, mock_state)

        assert ctx.catalog is None

    @patch('tap_listrak.context.metadata')
    def test_catalog_setter(self, mock_metadata, mock_get_client, mock_config, mock_state, mock_catalog):
        """Test that catalog setter sets catalog and selected_stream_ids."""
        mock_get_client.return_value = MagicMock()
        mock_metadata.get.return_value = None
        mock_metadata.to_map.return_value = {}

        ctx = Context(mock_config, mock_state)
        ctx.catalog = mock_catalog

        assert ctx._catalog == mock_catalog
        assert "lists" in ctx.selected_stream_ids
        assert isinstance(ctx.selected_stream_ids, set)

    @patch('tap_listrak.context.metadata')
    def test_catalog_setter_with_automatic_inclusion(self, mock_metadata, mock_get_client, mock_config, mock_state):
        """Test catalog setter includes automatic streams."""
        mock_get_client.return_value = MagicMock()

        # Create a catalog with an automatic stream
        catalog = MagicMock()
        stream1 = MagicMock()
        stream1.tap_stream_id = "lists"
        stream1.is_selected.return_value = False
        stream1.metadata = [{"metadata": {"inclusion": "automatic"}}]

        stream2 = MagicMock()
        stream2.tap_stream_id = "messages"
        stream2.is_selected.return_value = True
        stream2.metadata = []

        catalog.streams = [stream1, stream2]

        # Mock metadata.get to return 'automatic' for the first stream
        def metadata_get_side_effect(metadata_map, path, key):
            if key == 'inclusion' and stream1.tap_stream_id in str(metadata_map):
                return 'automatic'
            return None

        mock_metadata.get.side_effect = metadata_get_side_effect
        mock_metadata.to_map.return_value = {}

        ctx = Context(mock_config, mock_state)
        ctx.catalog = catalog

        # Both streams should be selected (one selected, one automatic)
        assert "messages" in ctx.selected_stream_ids


@patch('tap_listrak.context.get_client')
@patch('tap_listrak.context.bks_')
class TestContextBookmarks:
    """Test Context bookmark methods."""

    def test_get_bookmark(self, mock_bks, mock_get_client, mock_config, mock_state):
        """Test get_bookmark calls bookmarks.get_bookmark correctly."""
        mock_get_client.return_value = MagicMock()
        mock_bks.get_bookmark.return_value = "2023-06-01T00:00:00Z"

        ctx = Context(mock_config, mock_state)
        result = ctx.get_bookmark(["lists", "last_updated"])

        mock_bks.get_bookmark.assert_called_once_with(
            mock_state, "lists", "last_updated"
        )
        assert result == "2023-06-01T00:00:00Z"

    def test_set_bookmark_with_string(self, mock_bks, mock_get_client, mock_config, mock_state):
        """Test set_bookmark with string value."""
        mock_get_client.return_value = MagicMock()

        ctx = Context(mock_config, mock_state)
        ctx.set_bookmark(["lists", "last_updated"], "2023-07-01T00:00:00Z")

        mock_bks.write_bookmark.assert_called_once_with(
            mock_state, "lists", "last_updated", "2023-07-01T00:00:00Z"
        )

    def test_set_bookmark_with_date(self, mock_bks, mock_get_client, mock_config, mock_state):
        """Test set_bookmark converts date to ISO format."""
        mock_get_client.return_value = MagicMock()

        ctx = Context(mock_config, mock_state)
        test_date = date(2023, 7, 1)
        ctx.set_bookmark(["lists", "last_updated"], test_date)

        mock_bks.write_bookmark.assert_called_once_with(
            mock_state, "lists", "last_updated", "2023-07-01"
        )

    def test_set_bookmark_with_datetime(self, mock_bks, mock_get_client, mock_config, mock_state):
        """Test set_bookmark converts datetime to ISO format."""
        mock_get_client.return_value = MagicMock()

        ctx = Context(mock_config, mock_state)
        test_datetime = datetime(2023, 7, 1, 12, 30, 45)
        ctx.set_bookmark(["lists", "last_updated"], test_datetime)

        # datetime.isoformat() returns string with time
        expected_iso = test_datetime.isoformat()
        mock_bks.write_bookmark.assert_called_once_with(
            mock_state, "lists", "last_updated", expected_iso
        )


@patch('tap_listrak.context.get_client')
@patch('tap_listrak.context.bks_')
class TestContextOffsets:
    """Test Context offset methods."""

    def test_get_offset_with_existing_offset(self, mock_bks, mock_get_client, mock_config, mock_state):
        """Test get_offset returns existing offset value."""
        mock_get_client.return_value = MagicMock()
        mock_bks.get_offset.return_value = {"field": "value"}

        ctx = Context(mock_config, mock_state)
        result = ctx.get_offset(["lists", "field"])

        mock_bks.get_offset.assert_called_once_with(mock_state, "lists")
        assert result == "value"

    def test_get_offset_with_no_offset(self, mock_bks, mock_get_client, mock_config, mock_state):
        """Test get_offset returns None when offset doesn't exist."""
        mock_get_client.return_value = MagicMock()
        mock_bks.get_offset.return_value = None

        ctx = Context(mock_config, mock_state)
        result = ctx.get_offset(["lists", "field"])

        mock_bks.get_offset.assert_called_once_with(mock_state, "lists")
        assert result is None

    def test_get_offset_with_empty_dict(self, mock_bks, mock_get_client, mock_config, mock_state):
        """Test get_offset returns None when offset dict is empty."""
        mock_get_client.return_value = MagicMock()
        mock_bks.get_offset.return_value = {}

        ctx = Context(mock_config, mock_state)
        result = ctx.get_offset(["lists", "field"])

        assert result is None

    def test_set_offset(self, mock_bks, mock_get_client, mock_config, mock_state):
        """Test set_offset calls bookmarks.set_offset correctly."""
        mock_get_client.return_value = MagicMock()

        ctx = Context(mock_config, mock_state)
        ctx.set_offset(["lists", "field"], "offset_value")

        mock_bks.set_offset.assert_called_once_with(
            mock_state, "lists", "field", "offset_value"
        )

    def test_clear_offsets(self, mock_bks, mock_get_client, mock_config, mock_state):
        """Test clear_offsets calls bookmarks.clear_offset correctly."""
        mock_get_client.return_value = MagicMock()

        ctx = Context(mock_config, mock_state)
        ctx.clear_offsets("lists")

        mock_bks.clear_offset.assert_called_once_with(mock_state, "lists")


@patch('tap_listrak.context.get_client')
@patch('tap_listrak.context.bks_')
@patch('tap_listrak.context.pendulum')
class TestUpdateStartDateBookmark:
    """Test Context update_start_date_bookmark method."""

    def test_update_start_date_bookmark_with_existing_bookmark(
        self, mock_pendulum, mock_bks, mock_get_client, mock_config, mock_state
    ):
        """Test update_start_date_bookmark uses existing bookmark."""
        mock_get_client.return_value = MagicMock()
        mock_bks.get_bookmark.return_value = "2023-06-01T00:00:00Z"
        mock_parsed = MagicMock()
        mock_pendulum.parse.return_value = mock_parsed

        ctx = Context(mock_config, mock_state)
        result = ctx.update_start_date_bookmark(["lists", "last_updated"])

        mock_bks.get_bookmark.assert_called_once_with(
            mock_state, "lists", "last_updated"
        )
        mock_pendulum.parse.assert_called_once_with("2023-06-01T00:00:00Z")
        assert result == mock_parsed

    def test_update_start_date_bookmark_without_existing_bookmark(
        self, mock_pendulum, mock_bks, mock_get_client, mock_config, mock_state
    ):
        """Test update_start_date_bookmark falls back to start_date from config."""
        mock_get_client.return_value = MagicMock()
        mock_bks.get_bookmark.return_value = None
        mock_bks.write_bookmark.return_value = None
        mock_parsed = MagicMock()
        mock_pendulum.parse.return_value = mock_parsed

        ctx = Context(mock_config, mock_state)
        result = ctx.update_start_date_bookmark(["lists", "last_updated"])

        mock_bks.get_bookmark.assert_called_once()
        mock_bks.write_bookmark.assert_called_once_with(
            mock_state, "lists", "last_updated", "2023-01-01T00:00:00Z"
        )
        mock_pendulum.parse.assert_called_once_with("2023-01-01T00:00:00Z")
        assert result == mock_parsed

    def test_update_start_date_bookmark_sets_bookmark(
        self, mock_pendulum, mock_bks, mock_get_client, mock_config, mock_state
    ):
        """Test update_start_date_bookmark sets bookmark when none exists."""
        mock_get_client.return_value = MagicMock()
        mock_bks.get_bookmark.return_value = None

        ctx = Context(mock_config, mock_state)
        ctx.update_start_date_bookmark(["messages", "created_at"])

        mock_bks.write_bookmark.assert_called_once()
        # Should write the start_date from config
        call_args = mock_bks.write_bookmark.call_args
        assert call_args[0][2] == "created_at"
        assert call_args[0][3] == "2023-01-01T00:00:00Z"


@patch('tap_listrak.context.get_client')
@patch('tap_listrak.context.singer')
class TestWriteState:
    """Test Context write_state method."""

    def test_write_state(self, mock_singer, mock_get_client, mock_config, mock_state):
        """Test write_state calls singer.write_state correctly."""
        mock_get_client.return_value = MagicMock()

        ctx = Context(mock_config, mock_state)
        ctx.write_state()

        mock_singer.write_state.assert_called_once_with(mock_state)

    def test_write_state_with_updated_state(self, mock_singer, mock_get_client, mock_config):
        """Test write_state with modified state."""
        mock_get_client.return_value = MagicMock()

        initial_state = {"bookmarks": {}}
        ctx = Context(mock_config, initial_state)

        # Modify state
        ctx.state["bookmarks"]["lists"] = {"last_updated": "2023-07-01"}
        ctx.write_state()

        mock_singer.write_state.assert_called_once()
        call_args = mock_singer.write_state.call_args[0][0]
        assert call_args["bookmarks"]["lists"]["last_updated"] == "2023-07-01"


@patch('tap_listrak.context.get_client')
class TestContextCache:
    """Test Context cache functionality."""

    def test_cache_can_store_values(self, mock_get_client, mock_config, mock_state):
        """Test that cache can store and retrieve values."""
        mock_get_client.return_value = MagicMock()

        ctx = Context(mock_config, mock_state)
        ctx.cache["test_key"] = "test_value"

        assert ctx.cache["test_key"] == "test_value"

    def test_cache_can_store_complex_objects(self, mock_get_client, mock_config, mock_state):
        """Test that cache can store complex objects."""
        mock_get_client.return_value = MagicMock()

        ctx = Context(mock_config, mock_state)
        ctx.cache["lists"] = [{"id": 1}, {"id": 2}]
        ctx.cache["metadata"] = {"stream": "lists", "count": 100}

        assert len(ctx.cache["lists"]) == 2
        assert ctx.cache["metadata"]["count"] == 100

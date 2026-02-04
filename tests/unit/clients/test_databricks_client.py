import pandas as pd
import pytest
from tentaclio import URL

from tentaclio_databricks.clients.databricks_client import (
    DatabricksClient,
    DatabricksClientException,
)


class TestDatabricksClient:
    def setup_method(self):
        self.databricks_test_url = (
            "databricks+thrift://my_t0k3n@host.databricks.com"
            "?HTTPPath=/sql/1.0/endpoints/123456789"
        )
        self.client = DatabricksClient(URL(self.databricks_test_url))
        self.expected = pd.DataFrame({"id": [1, 2, 3]})

    @pytest.mark.parametrize(
        "url,server_hostname,http_path,access_token",
        [
            (
                "databricks+thrift://my_t0k3n@host.databricks.com"
                "?HTTPPath=/sql/1.0/endpoints/123456789",
                "host.databricks.com",
                "/sql/1.0/endpoints/123456789",
                "my_t0k3n",
            ),
            (
                "databricks+thrift://p@ssw0rd@host.databricks.co.uk"
                "?HTTPPath=/sql/1.0/endpoints/987654321",
                "host.databricks.co.uk",
                "/sql/1.0/endpoints/987654321",
                "p@ssw0rd",
            ),
        ],
    )
    def test_build_connection_dict(self, url, server_hostname, http_path, access_token):
        client = DatabricksClient(URL(url))
        assert client.server_hostname == server_hostname
        assert client.http_path == http_path
        assert client.access_token == access_token

    def test_get_df(self, mocker):
        client = self.client
        client.__enter__ = lambda _: client  # type: ignore
        client.query = lambda _: [1, 2, 3]  # type: ignore
        mocked_cursor = mocker.MagicMock()
        mocked_cursor.description = [("id", "int", None)]
        client.cursor = mocked_cursor
        df = client.get_df("foo")
        assert df.equals(self.expected)

    @pytest.mark.parametrize(
        "url",
        [
            "databricks+thrift://my_t0k3n@host.databricks.com",
            "databricks+thrift://shhhh@host.databricks.co.uk?http_path="
            "/sql/1.0/endpoints/987654321",
            "databricks+thrift://host.databricks.co.uk?HTTPPath=/sql/1.0/endpoints/987654321",
        ],
    )
    def test_error_http_path(self, url):
        with pytest.raises(DatabricksClientException):
            DatabricksClient(URL(url))

    def test_get_df_arrow_path(self, mocker):
        """Test that Arrow path is used when enabled."""
        client = self.client
        client.__enter__ = lambda _: client  # type: ignore

        mocked_cursor = mocker.MagicMock()
        mocked_table = mocker.MagicMock()
        mocked_table.to_pandas.return_value = self.expected
        mocked_cursor.fetchall_arrow.return_value = mocked_table
        client.cursor = mocked_cursor

        df = client.get_df("SELECT * FROM foo")

        # Verify execute was called with the query
        mocked_cursor.execute.assert_called_once_with("SELECT * FROM foo")
        # Verify Arrow fetch was used
        mocked_cursor.fetchall_arrow.assert_called_once()
        # Verify regular fetchall was NOT called
        mocked_cursor.fetchall.assert_not_called()
        assert df.equals(self.expected)

    def test_get_df_non_arrow_path(self, mocker):
        """Test that non-Arrow path is used when disabled."""
        client = DatabricksClient(URL(self.databricks_test_url), use_arrow=False)
        client.__enter__ = lambda _: client  # type: ignore

        mocked_cursor = mocker.MagicMock()
        mocked_cursor.fetchall.return_value = [(1,), (2,), (3,)]
        mocked_cursor.description = [("id", "int", None)]
        client.cursor = mocked_cursor

        df = client.get_df("SELECT * FROM foo")

        # Verify execute was called
        mocked_cursor.execute.assert_called_once_with("SELECT * FROM foo")
        # Verify regular fetchall was used
        mocked_cursor.fetchall.assert_called_once()
        # Verify Arrow fetch was NOT called
        assert not mocked_cursor.fetchall_arrow.called
        assert df.equals(self.expected)

    def test_get_df_arrow_fallback_on_attribute_error(self, mocker):
        """Test that Arrow falls back to regular fetch when not available."""
        client = self.client
        client.__enter__ = lambda _: client  # type: ignore

        mocked_cursor = mocker.MagicMock()
        # Simulate Arrow not being available
        mocked_cursor.fetchall_arrow.side_effect = AttributeError("fetchall_arrow not found")
        mocked_cursor.fetchall.return_value = [(1,), (2,), (3,)]
        mocked_cursor.description = [("id", "int", None)]
        client.cursor = mocked_cursor

        df = client.get_df("SELECT * FROM foo")

        # Verify Arrow was attempted first
        mocked_cursor.fetchall_arrow.assert_called_once()
        # Verify it fell back to regular fetchall
        mocked_cursor.fetchall.assert_called_once()
        assert df.equals(self.expected)

    def test_connection_uses_arrow_parameters(self, mocker):
        """Test that connection is created with Arrow parameters when enabled."""

        mock_connect = mocker.patch("databricks.sql.connect")
        mock_conn = mocker.MagicMock()
        mock_cursor = mocker.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        client = self.client
        client.__enter__()

        # Verify connection was called with Arrow parameters
        mock_connect.assert_called_once_with(
            server_hostname="host.databricks.com",
            http_path="/sql/1.0/endpoints/123456789",
            access_token="my_t0k3n",
            use_arrow_native_complex_types=True,
            use_arrow_native_decimals=True,
            use_arrow_native_timestamps=True,
        )
        # Verify cursor was created without arraysize
        mock_conn.cursor.assert_called_once_with()

    def test_connection_without_arrow_parameters(self, mocker):
        """Test that connection is created without Arrow parameters when disabled."""

        mock_connect = mocker.patch("databricks.sql.connect")
        mock_conn = mocker.MagicMock()
        mock_cursor = mocker.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        client = DatabricksClient(URL(self.databricks_test_url), use_arrow=False, arraysize=500000)
        client.__enter__()

        # Verify connection was called WITHOUT Arrow parameters
        mock_connect.assert_called_once_with(
            server_hostname="host.databricks.com",
            http_path="/sql/1.0/endpoints/123456789",
            access_token="my_t0k3n",
        )
        # Verify cursor was created with arraysize
        mock_conn.cursor.assert_called_once_with(arraysize=500000)

    def test_get_df_with_query_kwargs(self, mocker):
        """Test that query kwargs are passed through."""
        expected = pd.DataFrame({"id": [1]})
        client = self.client
        client.__enter__ = lambda _: client  # type: ignore

        mocked_cursor = mocker.MagicMock()
        mocked_table = mocker.MagicMock()
        mocked_table.to_pandas.return_value = expected
        mocked_cursor.fetchall_arrow.return_value = mocked_table
        client.cursor = mocked_cursor

        df = client.get_df("SELECT * FROM foo WHERE id = :id", parameters={"id": 1})

        # Verify kwargs were passed to execute
        mocked_cursor.execute.assert_called_once_with(
            "SELECT * FROM foo WHERE id = :id", parameters={"id": 1}
        )
        assert df.equals(expected)

    def test_get_df_empty_results_arrow(self, mocker):
        """Test Arrow path with empty results."""
        expected = pd.DataFrame()
        client = self.client
        client.__enter__ = lambda _: client  # type: ignore

        mocked_cursor = mocker.MagicMock()
        mocked_table = mocker.MagicMock()
        mocked_table.to_pandas.return_value = expected
        mocked_cursor.fetchall_arrow.return_value = mocked_table
        client.cursor = mocked_cursor

        df = client.get_df("SELECT * FROM foo WHERE 1=0")

        mocked_cursor.fetchall_arrow.assert_called_once()
        assert len(df) == 0

    def test_get_df_empty_results_non_arrow(self, mocker):
        """Test non-Arrow path with empty results."""
        client = DatabricksClient(URL(self.databricks_test_url), use_arrow=False)
        client.__enter__ = lambda _: client  # type: ignore

        mocked_cursor = mocker.MagicMock()
        mocked_cursor.fetchall.return_value = []
        mocked_cursor.description = [("id", "int", None)]
        client.cursor = mocked_cursor

        df = client.get_df("SELECT * FROM foo WHERE 1=0")

        mocked_cursor.fetchall.assert_called_once()
        assert len(df) == 0

    def test_arrow_vs_legacy_types(self, mocker):
        """
        Ensure Arrow, Arrow-fallback, and non-Arrow paths return equivalent
        DataFrames with the same values, columns, and dtypes.
        """

        expected = pd.DataFrame({"id": pd.Series([1, 2, 3], dtype="int64")})

        # -------------------------
        # Arrow native path
        # -------------------------
        client_arrow = self.client
        client_arrow.__enter__ = lambda _: client_arrow  # type: ignore

        mocked_cursor_arrow = mocker.MagicMock()
        mocked_table = mocker.MagicMock()
        mocked_table.to_pandas.return_value = expected.copy()
        mocked_cursor_arrow.fetchall_arrow.return_value = mocked_table
        client_arrow.cursor = mocked_cursor_arrow

        df_arrow = client_arrow.get_df("SELECT * FROM foo")

        # -------------------------
        # Arrow fallback path
        # -------------------------
        client_arrow_fallback = DatabricksClient(URL(self.databricks_test_url), use_arrow=True)
        client_arrow_fallback.__enter__ = lambda _: client_arrow_fallback  # type: ignore

        mocked_cursor_fallback = mocker.MagicMock()
        mocked_cursor_fallback.fetchall_arrow.side_effect = AttributeError(
            "fetchall_arrow not available"
        )
        mocked_cursor_fallback.fetchall.return_value = [(1,), (2,), (3,)]
        mocked_cursor_fallback.description = [("id", "int", None)]
        client_arrow_fallback.cursor = mocked_cursor_fallback

        df_arrow_fallback = client_arrow_fallback.get_df("SELECT * FROM foo")

        # -------------------------
        # Non-arrow path
        # -------------------------
        client_non_arrow = DatabricksClient(URL(self.databricks_test_url), use_arrow=False)
        client_non_arrow.__enter__ = lambda _: client_non_arrow  # type: ignore

        mocked_cursor_non_arrow = mocker.MagicMock()
        mocked_cursor_non_arrow.fetchall.return_value = [(1,), (2,), (3,)]
        mocked_cursor_non_arrow.description = [("id", "int", None)]
        client_non_arrow.cursor = mocked_cursor_non_arrow

        df_non_arrow = client_non_arrow.get_df("SELECT * FROM foo")

        # -------------------------
        # Assertions: values
        # -------------------------
        assert df_arrow.equals(expected)
        assert df_arrow_fallback.equals(expected)
        assert df_non_arrow.equals(expected)

        # -------------------------
        # Assertions: dtypes
        # -------------------------
        assert df_arrow.dtypes.to_dict() == expected.dtypes.to_dict()
        assert df_arrow_fallback.dtypes.to_dict() == expected.dtypes.to_dict()
        assert df_non_arrow.dtypes.to_dict() == expected.dtypes.to_dict()

        # -------------------------
        # Assertions: columns
        # -------------------------
        assert list(df_arrow.columns) == ["id"]
        assert list(df_arrow_fallback.columns) == ["id"]
        assert list(df_non_arrow.columns) == ["id"]

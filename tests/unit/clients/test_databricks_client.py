import pandas as pd
import pytest
from tentaclio import URL

from tentaclio_databricks.clients.databricks_client import (
    DatabricksClient,
    DatabricksClientException,
)


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
def test_build_connection_dict(url, server_hostname, http_path, access_token):
    client = DatabricksClient(URL(url))
    assert client.server_hostname == server_hostname
    assert client.http_path == http_path
    assert client.access_token == access_token


def test_get_df(mocker):
    expected = pd.DataFrame({"id": [1, 2, 3]})
    url = "databricks+thrift://my_t0k3n@host.databricks.com?HTTPPath=/sql/1.0/endpoints/123456789"
    client = DatabricksClient(URL(url))
    client.__enter__ = lambda _: client  # type: ignore
    client.query = lambda _: [1, 2, 3]  # type: ignore
    mocked_cursor = mocker.MagicMock()
    mocked_cursor.description = [("id", "int", None)]
    client.cursor = mocked_cursor
    df = client.get_df("foo")
    assert df.equals(expected)


@pytest.mark.parametrize(
    "url",
    [
        "databricks+thrift://my_t0k3n@host.databricks.com",
        "databricks+thrift://shhhh@host.databricks.co.uk?http_path="
        "/sql/1.0/endpoints/987654321",
        "databricks+thrift://host.databricks.co.uk?HTTPPath=/sql/1.0/endpoints/987654321",
    ],
)
def test_error_http_path(url):
    with pytest.raises(DatabricksClientException):
        DatabricksClient(URL(url))


class TestQueryComments:
    """Tests for query comment functionality."""

    def test_build_comment_with_multiple_annotations(self):
        url = "databricks+thrift://token@host.databricks.com?HTTPPath=/sql/1.0/endpoints/123"
        client = DatabricksClient(
            URL(url), query_annotations={"app_name": "JupyterHub", "user": "john"}
        )
        comment = client._build_query_comment()
        assert "app_name='JupyterHub'" in comment
        assert "user='john'" in comment

    def test_build_comment_with_single_annotation(self):
        url = "databricks+thrift://token@host.databricks.com?HTTPPath=/sql/1.0/endpoints/123"
        client = DatabricksClient(URL(url), query_annotations={"app_name": "JupyterHub"})
        comment = client._build_query_comment()
        assert comment == "/* app_name='JupyterHub' */\n"

    def test_build_comment_no_annotations(self):
        url = "databricks+thrift://token@host.databricks.com?HTTPPath=/sql/1.0/endpoints/123"
        client = DatabricksClient(URL(url))
        comment = client._build_query_comment()
        assert comment == ""

    def test_build_comment_escapes_single_quotes(self):
        url = "databricks+thrift://token@host.databricks.com?HTTPPath=/sql/1.0/endpoints/123"
        client = DatabricksClient(
            URL(url), query_annotations={"app_name": "john's App", "user": "O'Reilly"}
        )
        comment = client._build_query_comment()
        assert "app_name='john''s App'" in comment
        assert "user='O''Reilly'" in comment

    def test_prepend_comment_with_annotations(self):
        url = "databricks+thrift://token@host.databricks.com?HTTPPath=/sql/1.0/endpoints/123"
        client = DatabricksClient(URL(url), query_annotations={"app_name": "JupyterHub"})
        query = "SELECT * FROM table"
        result = client._prepend_comment(query)
        assert result.startswith("/* app_name='JupyterHub' */\n")
        assert "SELECT * FROM table" in result

    def test_prepend_comment_without_annotations(self):
        url = "databricks+thrift://token@host.databricks.com?HTTPPath=/sql/1.0/endpoints/123"
        client = DatabricksClient(URL(url))
        query = "SELECT * FROM table"
        result = client._prepend_comment(query)
        assert result == query

    def test_query_prepends_comment(self, mocker):
        url = "databricks+thrift://token@host.databricks.com?HTTPPath=/sql/1.0/endpoints/123"
        client = DatabricksClient(URL(url), query_annotations={"app_name": "TestApp"})

        # Mock cursor
        mock_cursor = mocker.MagicMock()
        mock_cursor.fetchall.return_value = [(1,)]
        client.cursor = mock_cursor

        client.query("SELECT 1")

        # Verify execute was called with prepended comment
        call_args = mock_cursor.execute.call_args[0][0]
        assert call_args.startswith("/* app_name='TestApp' */\n")
        assert "SELECT 1" in call_args

    def test_execute_prepends_comment(self, mocker):
        url = "databricks+thrift://token@host.databricks.com?HTTPPath=/sql/1.0/endpoints/123"
        client = DatabricksClient(URL(url), query_annotations={"app_name": "TestApp"})

        # Mock cursor
        mock_cursor = mocker.MagicMock()
        client.cursor = mock_cursor

        client.execute("CREATE TABLE foo (id INT)")

        # Verify execute was called with prepended comment
        call_args = mock_cursor.execute.call_args[0][0]
        assert call_args.startswith("/* app_name='TestApp' */\n")
        assert "CREATE TABLE foo" in call_args

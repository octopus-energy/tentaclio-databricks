import pandas as pd
import pytest
from tentaclio import URL
from decimal import Decimal
from datetime import date, datetime

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

        expected = pd.DataFrame(
            {
                "id": pd.Series([1, 2, 3], dtype="int64"),
                "score": pd.Series([1.5, 2.5, 3.5], dtype="float64"),
                "flag": pd.Series([True, False, True], dtype="bool"),
                "event_time": pd.Series(
                    [
                        pd.Timestamp("2024-01-01 00:00:00"),
                        pd.Timestamp("2024-01-02 00:00:00"),
                        pd.Timestamp("2024-01-03 00:00:00"),
                    ],
                    dtype="datetime64[ns]",
                ),
                "note": pd.Series(["a", "b", "c"], dtype="object"),
            }
        )
        rows = [
            (1, 1.5, True, pd.Timestamp("2024-01-01 00:00:00"), "a"),
            (2, 2.5, False, pd.Timestamp("2024-01-02 00:00:00"), "b"),
            (3, 3.5, True, pd.Timestamp("2024-01-03 00:00:00"), "c"),
        ]
        columns = ["id", "score", "flag", "event_time", "note"]

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
        mocked_cursor_fallback.fetchall.return_value = rows
        mocked_cursor_fallback.description = [(name, None, None) for name in columns]
        client_arrow_fallback.cursor = mocked_cursor_fallback

        df_arrow_fallback = client_arrow_fallback.get_df("SELECT * FROM foo")

        # -------------------------
        # Non-arrow path
        # -------------------------
        client_non_arrow = DatabricksClient(URL(self.databricks_test_url), use_arrow=False)
        client_non_arrow.__enter__ = lambda _: client_non_arrow  # type: ignore

        mocked_cursor_non_arrow = mocker.MagicMock()
        mocked_cursor_non_arrow.fetchall.return_value = rows
        mocked_cursor_non_arrow.description = [(name, None, None) for name in columns]
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
        assert list(df_arrow.columns) == columns
        assert list(df_arrow_fallback.columns) == columns
        assert list(df_non_arrow.columns) == columns

    def test_comprehensive_types_arrow_vs_legacy(self, mocker):
        """
        Test comprehensive data type coverage including:
        - Date types
        - Decimal types with precision
        - Binary data
        - NULL values
        - Array/List types
        - Map/Dictionary types
        - Struct/Nested types
        - Large integers (BIGINT)
        - Small integers (TINYINT, SMALLINT)
        """

        expected = pd.DataFrame(
            {
                "bigint_col": pd.Series(
                    [9223372036854775807, -9223372036854775808, 0], dtype="int64"
                ),
                "int_col": pd.Series([2147483647, -2147483648, 0], dtype="int64"),
                "smallint_col": pd.Series([32767, -32768, 0], dtype="int64"),
                "tinyint_col": pd.Series([127, -128, 0], dtype="int64"),
                "double_col": pd.Series(
                    [1.7976931348623157e308, 2.2250738585072014e-308, 0.0], dtype="float64"
                ),
                "float_col": pd.Series([3.4028235e38, 1.1754944e-38, 0.0], dtype="float64"),
                "decimal_col": pd.Series(
                    [Decimal("999.99"), Decimal("0.01"), Decimal("0.00")], dtype="object"
                ),
                "string_col": pd.Series(["hello", "world", ""], dtype="object"),
                "boolean_col": pd.Series([True, False, None], dtype="object"),
                "date_col": pd.Series(
                    [date(2024, 1, 1), date(2024, 12, 31), date(2000, 1, 1)], dtype="object"
                ),
                "timestamp_col": pd.Series(
                    [
                        datetime(2024, 1, 1, 12, 30, 45, 123456),
                        datetime(2024, 12, 31, 23, 59, 59, 999999),
                        datetime(2000, 1, 1, 0, 0, 0, 0),
                    ],
                    dtype="datetime64[ns]",
                ),
                "binary_col": pd.Series([b"binary_data", b"\x00\x01\x02", b""], dtype="object"),
                "array_col": pd.Series([[1, 2, 3], [4, 5], []], dtype="object"),
                "map_col": pd.Series(
                    [{"key1": "val1", "key2": "val2"}, {"a": "b"}, {}], dtype="object"
                ),
                "struct_col": pd.Series(
                    [
                        {"field1": 1, "field2": "a"},
                        {"field1": 2, "field2": "b"},
                        {"field1": 3, "field2": "c"},
                    ],
                    dtype="object",
                ),
                "null_string_col": pd.Series([None, "not null", None], dtype="object"),
                "null_int_col": pd.Series([None, 42, None], dtype="object"),
            }
        )

        rows = [
            (
                9223372036854775807,
                2147483647,
                32767,
                127,
                1.7976931348623157e308,
                3.4028235e38,
                Decimal("999.99"),
                "hello",
                True,
                date(2024, 1, 1),
                datetime(2024, 1, 1, 12, 30, 45, 123456),
                b"binary_data",
                [1, 2, 3],
                {"key1": "val1", "key2": "val2"},
                {"field1": 1, "field2": "a"},
                None,
                None,
            ),
            (
                -9223372036854775808,
                -2147483648,
                -32768,
                -128,
                2.2250738585072014e-308,
                1.1754944e-38,
                Decimal("0.01"),
                "world",
                False,
                date(2024, 12, 31),
                datetime(2024, 12, 31, 23, 59, 59, 999999),
                b"\x00\x01\x02",
                [4, 5],
                {"a": "b"},
                {"field1": 2, "field2": "b"},
                "not null",
                42,
            ),
            (
                0,
                0,
                0,
                0,
                0.0,
                0.0,
                Decimal("0.00"),
                "",
                None,
                date(2000, 1, 1),
                datetime(2000, 1, 1, 0, 0, 0, 0),
                b"",
                [],
                {},
                {"field1": 3, "field2": "c"},
                None,
                None,
            ),
        ]

        columns = [
            "bigint_col",
            "int_col",
            "smallint_col",
            "tinyint_col",
            "double_col",
            "float_col",
            "decimal_col",
            "string_col",
            "boolean_col",
            "date_col",
            "timestamp_col",
            "binary_col",
            "array_col",
            "map_col",
            "struct_col",
            "null_string_col",
            "null_int_col",
        ]

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

        df_arrow = client_arrow.get_df("SELECT * FROM comprehensive_types")
        # -------------------------
        # Non-arrow path
        # -------------------------
        client_non_arrow = DatabricksClient(URL(self.databricks_test_url), use_arrow=False)
        client_non_arrow.__enter__ = lambda _: client_non_arrow  # type: ignore

        mocked_cursor_non_arrow = mocker.MagicMock()
        mocked_cursor_non_arrow.fetchall.return_value = rows
        mocked_cursor_non_arrow.description = [(name, None, None) for name in columns]
        client_non_arrow.cursor = mocked_cursor_non_arrow

        df_non_arrow = client_non_arrow.get_df("SELECT * FROM comprehensive_types")

        # -------------------------
        # Assertions: shape
        # -------------------------
        assert df_arrow.shape == expected.shape
        assert df_non_arrow.shape == expected.shape

        # -------------------------
        # Assertions: columns
        # -------------------------
        assert list(df_arrow.columns) == columns
        assert list(df_non_arrow.columns) == columns

        # -------------------------
        # Assertions: values (column by column)
        # -------------------------
        for col in columns:
            # For numeric columns, use direct equality
            if col in [
                "bigint_col",
                "int_col",
                "smallint_col",
                "tinyint_col",
                "double_col",
                "float_col",
            ]:
                pd.testing.assert_series_equal(df_arrow[col], expected[col], check_names=True)
                pd.testing.assert_series_equal(df_non_arrow[col], expected[col], check_names=True)

            # For timestamp columns
            elif col == "timestamp_col":
                pd.testing.assert_series_equal(df_arrow[col], expected[col], check_names=True)
                pd.testing.assert_series_equal(df_non_arrow[col], expected[col], check_names=True)

            # For other columns, check equality element by element
            else:
                for idx in range(len(expected)):
                    assert df_arrow[col].iloc[idx] == expected[col].iloc[idx] or (
                        pd.isna(df_arrow[col].iloc[idx]) and pd.isna(expected[col].iloc[idx])
                    )
                    assert df_non_arrow[col].iloc[idx] == expected[col].iloc[idx] or (
                        pd.isna(df_non_arrow[col].iloc[idx]) and pd.isna(expected[col].iloc[idx])
                    )

    def test_null_values_handling(self, mocker):
        """
        Test that NULL values are properly handled across all three paths
        for various data types.
        """

        expected = pd.DataFrame(
            {
                "id": pd.Series([1, None, 3], dtype="object"),
                "name": pd.Series([None, "test", None], dtype="object"),
                "amount": pd.Series([None, 100.5, None], dtype="object"),
                "is_active": pd.Series([True, None, False], dtype="object"),
                "created_at": pd.Series([None, pd.Timestamp("2024-01-01"), None], dtype="object"),
            }
        )

        rows = [
            (1, None, None, True, None),
            (None, "test", 100.5, None, pd.Timestamp("2024-01-01")),
            (3, None, None, False, None),
        ]

        columns = ["id", "name", "amount", "is_active", "created_at"]

        # Test Arrow path
        client_arrow = self.client
        client_arrow.__enter__ = lambda _: client_arrow  # type: ignore

        mocked_cursor_arrow = mocker.MagicMock()
        mocked_table = mocker.MagicMock()
        mocked_table.to_pandas.return_value = expected.copy()
        mocked_cursor_arrow.fetchall_arrow.return_value = mocked_table
        client_arrow.cursor = mocked_cursor_arrow

        df_arrow = client_arrow.get_df("SELECT * FROM nulls_test")

        # Test fallback path
        client_fallback = DatabricksClient(URL(self.databricks_test_url), use_arrow=True)
        client_fallback.__enter__ = lambda _: client_fallback  # type: ignore

        mocked_cursor_fallback = mocker.MagicMock()
        mocked_cursor_fallback.fetchall_arrow.side_effect = AttributeError()
        mocked_cursor_fallback.fetchall.return_value = rows
        mocked_cursor_fallback.description = [(name, None, None) for name in columns]
        client_fallback.cursor = mocked_cursor_fallback

        df_fallback = client_fallback.get_df("SELECT * FROM nulls_test")

        # Test non-arrow path
        client_non_arrow = DatabricksClient(URL(self.databricks_test_url), use_arrow=False)
        client_non_arrow.__enter__ = lambda _: client_non_arrow  # type: ignore

        mocked_cursor_non_arrow = mocker.MagicMock()
        mocked_cursor_non_arrow.fetchall.return_value = rows
        mocked_cursor_non_arrow.description = [(name, None, None) for name in columns]
        client_non_arrow.cursor = mocked_cursor_non_arrow

        df_non_arrow = client_non_arrow.get_df("SELECT * FROM nulls_test")

        # Verify NULL handling
        for col in columns:
            for idx in range(len(expected)):
                expected_val = expected[col].iloc[idx]
                arrow_val = df_arrow[col].iloc[idx]
                fallback_val = df_fallback[col].iloc[idx]
                non_arrow_val = df_non_arrow[col].iloc[idx]

                if pd.isna(expected_val):
                    assert pd.isna(arrow_val)
                    assert pd.isna(fallback_val)
                    assert pd.isna(non_arrow_val)
                else:
                    assert arrow_val == expected_val
                    assert fallback_val == expected_val
                    assert non_arrow_val == expected_val

    def test_edge_case_values(self, mocker):
        """
        Test edge cases like very long strings, special characters,
        unicode, empty strings, extreme numeric values.
        """

        long_string = "a" * 10000
        unicode_string = "Hello 世界 🌍 émojis"
        special_chars = "!@#$%^&*()_+-=[]{}|;':\",./<>?"

        expected = pd.DataFrame(
            {
                "long_text": pd.Series([long_string, "", long_string], dtype="object"),
                "unicode_text": pd.Series(
                    [unicode_string, "ASCII", unicode_string], dtype="object"
                ),
                "special_chars": pd.Series([special_chars, "", special_chars], dtype="object"),
                "zero": pd.Series([0, 0, 0], dtype="int64"),
                "negative_zero": pd.Series([-0.0, 0.0, -0.0], dtype="float64"),
                "inf": pd.Series([float("inf"), float("-inf"), 0.0], dtype="float64"),
            }
        )

        rows = [
            (long_string, unicode_string, special_chars, 0, -0.0, float("inf")),
            ("", "ASCII", "", 0, 0.0, float("-inf")),
            (long_string, unicode_string, special_chars, 0, -0.0, 0.0),
        ]

        columns = ["long_text", "unicode_text", "special_chars", "zero", "negative_zero", "inf"]

        # Test all three paths
        for use_arrow, force_fallback in [(True, False), (True, True), (False, False)]:
            client = DatabricksClient(URL(self.databricks_test_url), use_arrow=use_arrow)
            client.__enter__ = lambda _: client  # type: ignore

            mocked_cursor = mocker.MagicMock()

            if use_arrow and not force_fallback:
                mocked_table = mocker.MagicMock()
                mocked_table.to_pandas.return_value = expected.copy()
                mocked_cursor.fetchall_arrow.return_value = mocked_table
            elif use_arrow and force_fallback:
                mocked_cursor.fetchall_arrow.side_effect = AttributeError()
                mocked_cursor.fetchall.return_value = rows
                mocked_cursor.description = [(name, None, None) for name in columns]
            else:
                mocked_cursor.fetchall.return_value = rows
                mocked_cursor.description = [(name, None, None) for name in columns]

            client.cursor = mocked_cursor
            df = client.get_df("SELECT * FROM edge_cases")

            # Verify the DataFrame matches expected
            assert df.shape == expected.shape
            assert list(df.columns) == columns

            # Check string columns
            for col in ["long_text", "unicode_text", "special_chars"]:
                for idx in range(len(expected)):
                    assert df[col].iloc[idx] == expected[col].iloc[idx]

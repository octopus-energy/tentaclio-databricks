"""Databricks query client."""

from typing import Any, Dict, List, Optional

import pandas as pd
from databricks import sql
from tentaclio import URL


class DatabricksClientException(Exception):
    """Databricks client specific exception."""


class DatabricksClient:
    """Databricks client, backed by an Apache Thrift connection."""

    def __init__(
        self,
        url: URL,
        arraysize: int = 1000000,
        use_arrow: bool = True,
        query_annotations: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        # This is a very common issue reported by the users
        if url.query is None or "HTTPPath" not in url.query:
            raise DatabricksClientException(
                "Missing the HTTPPath element in the http query. \n\n"
                "The url should look like: "
                "databricks+thrift://workspaceurl.databricks.com/?HTTPPath=value\n"
                "Check the connection details of your databricks warehouse"
            )
        if url.username is None or url.username == "":
            raise DatabricksClientException(
                "Missing the token in the url:\n\n"
                "The url should look like: "
                "databricks+thrift://token@workspaceurl.databricks.com/?HTTPPath=value"
            )

        self.server_hostname = url.hostname
        self.http_path = url.query["HTTPPath"]
        self.access_token = url.username
        self.arraysize = arraysize
        self.use_arrow = use_arrow
        self.query_annotations = query_annotations or {}

    def __enter__(self):
        conn_kwargs = {
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
            "access_token": self.access_token,
        }

        if self.use_arrow:
            conn_kwargs.update(
                {
                    "use_arrow_native_complex_types": True,
                    "use_arrow_native_decimals": True,
                    "use_arrow_native_timestamps": True,
                }
            )

        self.conn = sql.connect(**conn_kwargs)

        # Only set arraysize if not using Arrow
        if self.use_arrow:
            self.cursor = self.conn.cursor()
        else:
            self.cursor = self.conn.cursor(arraysize=self.arraysize)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    def _build_query_comment(self) -> str:
        """Build a SQL comment from query annotations.

        Returns:
            Formatted SQL comment string, or empty string if no annotations.
        """
        if not self.query_annotations:
            return ""

        parts = []
        for key, value in self.query_annotations.items():
            # Escape single quotes in values
            escaped_value = value.replace("'", "''")
            parts.append(f"{key}='{escaped_value}'")

        if parts:
            return f"/* {', '.join(parts)} */\n"
        return ""

    def _prepend_comment(self, sql_query: str) -> str:
        """Prepend query comment to SQL query if configured.

        Args:
            sql_query: The SQL query string.

        Returns:
            SQL query with prepended comment if annotations configured, else original query.
        """
        comment = self._build_query_comment()
        if comment:
            return f"{comment}{sql_query}"
        return sql_query

    def query(self, sql_query: str, **kwargs) -> List[Any]:
        """Execute a SQL query, and return results."""
        sql_query = self._prepend_comment(sql_query)
        self.cursor.execute(sql_query, **kwargs)
        return self.cursor.fetchall()

    def execute(self, sql_query: str, **kwargs) -> None:
        """Execute a raw SQL query command."""
        sql_query = self._prepend_comment(sql_query)
        self.cursor.execute(sql_query, **kwargs)

    def get_df(self, sql_query: str, **kwargs) -> pd.DataFrame:
        """Run a raw SQL query and return a data frame."""
        self.cursor.execute(sql_query, **kwargs)

        if self.use_arrow:
            # Default to Arrow for fast data transfer
            try:
                arrow_table = self.cursor.fetchall_arrow()
                return arrow_table.to_pandas()
            except AttributeError:
                data = self.cursor.fetchall()
        else:
            # Original row-by-row method
            data = self.cursor.fetchall()

        # Build DataFrame from rows (fallback path)
        columns = (
            [col_desc[0] for col_desc in self.cursor.description]
            if self.cursor.description
            else []
        )
        return pd.DataFrame(data, columns=columns)

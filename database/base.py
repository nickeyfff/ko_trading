import os
from threading import Lock
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

db_path = os.environ.get("DBPATH", "")


class DuckDBBase:
    table_name: str

    def __init__(self):
        self.db_name = db_path
        self.conn = duckdb.connect(self.db_name)
        self._lock = Lock()

    def _execute(self, query: str, params: tuple = ()) -> Any:
        """Execute SQL query with thread-local cursor"""
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return cursor

    def create_table(self, table_name: str, columns: Dict[str, str]):
        """Create a table"""
        columns_def = ", ".join(
            f"{col} {col_type}" for col, col_type in columns.items()
        )
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})"
        with self._lock:  # Ensure table creation is thread-safe
            self._execute(query)

    def insert_dataframe(self, table_name: str, df: pd.DataFrame):
        if df.empty:
            return

        temp_view_name = f"temp_{table_name}_view"
        self.conn.register(temp_view_name, df)

        query = f"INSERT INTO {table_name} SELECT * FROM {temp_view_name}"

        with self._lock:
            self.conn.execute(query)
            self.conn.commit()
        self.conn.unregister(temp_view_name)
        return len(df)

    def select(
        self,
        table_name: str,
        conditions: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Query records and return Pandas DataFrame in a thread-safe manner"""
        cursor = self.conn.cursor()
        fields_str = "*" if not fields else ", ".join(fields)
        query = f"SELECT {fields_str} FROM {table_name}"
        params = ()

        if conditions:
            where_clause = " AND ".join(f"{k}=?" for k in conditions.keys())
            query += f" WHERE {where_clause}"
            params = tuple(conditions.values())

        df = cursor.execute(query, params).fetch_df()
        return df

    def delete(self, table_name: str, conditions: Dict[str, Any]) -> int:
        """Delete records"""
        where_clause = " AND ".join(f"{k}=?" for k in conditions.keys())
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        cursor = self._execute(query, tuple(conditions.values()))
        return cursor.rowcount

    def truncate_table(self, table_name: str) -> int:
        """Truncate all data in the table, preserving the table structure"""
        query = f"DELETE FROM {table_name}"
        cursor = self._execute(query)
        return cursor.rowcount

    def query_df(self, sql):
        with self.conn.cursor() as cursor:
            df = cursor.execute(sql).fetch_df()
        return df

    def get_latest_date(self) -> str | None:
        try:
            df = self.query_df(f"SELECT MAX(date) AS latest FROM {self.table_name}")
        except Exception:
            return None

        if df.empty:
            return None

        val = df.iloc[0, 0]
        if pd.isna(val):
            return None

        # 正常情况：返回字符串格式日期
        return (
            pd.to_datetime(val).strftime("%Y-%m-%d")
            if not isinstance(val, str)
            else val
        )


db = DuckDBBase()

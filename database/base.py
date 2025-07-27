import os
from threading import Lock
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

db_path = os.environ.get("DBPATH", "")


class DuckDBBase:
    def __init__(self):
        self.db_name = db_path
        self.conn = duckdb.connect(self.db_name)
        # Optional: Lock for connection initialization if needed
        self._lock = Lock()

    def _execute(self, query: str, params: tuple = ()) -> Any:
        """Execute SQL query with thread-local cursor"""
        cursor = self.conn.cursor()  # Create a new cursor for each execution
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

    def insert(self, table_name: str, data: Dict[str, Any]) -> int:
        """Insert a single record"""
        columns = ", ".join(data.keys())
        placeholders = ", ".join("?" * len(data))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor = self._execute(query, tuple(data.values()))
        return cursor.lastrowid

    def insert_dataframe(self, table_name: str, df: pd.DataFrame) -> int:
        """Insert DataFrame data in bulk"""
        if df.empty:
            return 0

        cursor = self.conn.cursor()
        columns = ", ".join(df.columns)
        placeholders = ", ".join("?" * len(df.columns))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            self.conn.commit()
            cursor.executemany(query, df.values.tolist())
            return len(df)
        except Exception as e:
            self.conn.rollback()
            print(f"Error inserting data: {e}")
            raise

    def select(
        self,
        table_name: str,
        conditions: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Query records and return Pandas DataFrame in a thread-safe manner"""
        # Create a thread-local cursor for this select operation
        cursor = self.conn.cursor()
        fields_str = "*" if not fields else ", ".join(fields)
        query = f"SELECT {fields_str} FROM {table_name}"
        params = ()

        if conditions:
            where_clause = " AND ".join(f"{k}=?" for k in conditions.keys())
            query += f" WHERE {where_clause}"
            params = tuple(conditions.values())

        # Execute query using thread-local cursor and fetch results as DataFrame
        df = cursor.execute(query, params).fetch_df()
        return df

    def update(
        self, table_name: str, data: Dict[str, Any], conditions: Dict[str, Any]
    ) -> int:
        """Update records"""
        set_clause = ", ".join(f"{k}=?" for k in data.keys())
        where_clause = " AND ".join(f"{k}=?" for k in conditions.keys())
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        params = tuple(data.values()) + tuple(conditions.values())
        cursor = self._execute(query, params)
        return cursor.rowcount

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

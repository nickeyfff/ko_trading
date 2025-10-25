from datetime import datetime

from dateutil.relativedelta import relativedelta

from database.base import DuckDBBase


class Stock(DuckDBBase):
    def __init__(self):
        super().__init__()
        self.table_name = "v_qfq_stocks"

    def query(self, symbol):
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE symbol = ?
        """
        params = [symbol]

        with self.conn.cursor() as cursor:
            df = cursor.execute(query, params).fetch_df()
        return df

    def list_new_stocks(self, years_ago=2):
        """
        查询新股：最近两年（从当前日期起）开始有记录的股票。
        返回：包含 symbol, first_date 和 stock_type 的 DataFrame。
        """
        d = (datetime.now() - relativedelta(years=years_ago)).strftime("%Y-%m-%d")
        query = f"""
        WITH first_record AS (
            SELECT
                symbol,
                MIN(date) AS first_date
            FROM {self.table_name}
            GROUP BY symbol
        )
        SELECT
            symbol,
            first_date
        FROM first_record
        WHERE first_date >= '{d}'
        ORDER BY first_date DESC
        """

        with self.conn.cursor() as cursor:
            df = cursor.execute(query).fetch_df()
        return df


stock = Stock()

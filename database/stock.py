from datetime import datetime
from typing import Optional

import pandas as pd
from dateutil.relativedelta import relativedelta

from common import generate_symbol
from database.base import DuckDBBase


class Stock(DuckDBBase):
    def __init__(self):
        super().__init__()
        self.qfq_table_name = "v_qfq_stocks"
        self.xdxr_table_name = "v_xdxr"

    def query(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        # 1. 构建WHERE子句的各个条件
        conditions = []
        if start_date:
            conditions.append(f" AND date >= '{start_date}'")

        if end_date:
            conditions.append(f" AND date <= '{end_date}'")

        # 2. 组合成最终的WHERE子句
        where_clause = f"WHERE symbol='{symbol}'"
        if conditions:
            where_clause += f"{' '.join(conditions)}"

        # 3. 构建完整的SQL查询
        query = f"""
            SELECT *
            FROM {self.qfq_table_name}
            {where_clause}
            ORDER BY date;
        """

        return self.query_df(query)

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
            FROM {self.qfq_table_name}
            GROUP BY symbol
        )
        SELECT
            symbol,
            first_date
        FROM first_record
        WHERE first_date >= '{d}'
        ORDER BY first_date DESC
        """
        return self.query_df(sql=query)

    def get_available_dates(self):
        """获取交易日"""
        sql = f"SELECT DISTINCT date FROM {self.qfq_table_name} ORDER BY date DESC"
        df = self.query_df(sql)
        return pd.to_datetime(df["date"]).dt.date.tolist()

    def list_stocks_with_xdxr(self, start_date):
        end_date = self.get_latest_date()
        query = f"select distinct code from '{self.xdxr_table_name}' where date>='{start_date}' and date<='{end_date}';"
        df = self.query_df(query)
        df["symbol"] = df["code"].apply(generate_symbol)
        symbols = [s for s in df["symbol"]]
        return symbols


stock = Stock()

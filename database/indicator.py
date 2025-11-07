from typing import Optional

import pandas as pd

from database.base import DuckDBBase


class Indicator(DuckDBBase):
    def __init__(self):
        super().__init__()
        self.table_name = "calc_indicator"
        self._create_indicator_table()

    def _create_indicator_table(self):
        """建表"""
        columns = {
            "date": "DATE",
            "symbol": "VARCHAR",
            "indicator": "VARCHAR",
            "value": "DOUBLE",
        }
        super().create_table(self.table_name, columns)

    def insert(self, df: pd.DataFrame):
        required_cols = ["date", "symbol", "indicator", "value"]
        if not set(required_cols).issubset(df.columns):
            raise ValueError(f"DataFrame 必须包含 {required_cols} 四列")

        df = df[required_cols].copy()
        self.insert_dataframe(table_name=self.table_name, df=df)

    def delete_symbols(self, symbols):
        symbol_str = ", ".join([f"'{s}'" for s in symbols])
        sql = f"DELETE FROM {self.table_name} WHERE symbol in ({symbol_str})"
        self._execute(query=sql)

    def query(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        查询指标数据并转换成宽表。所有参数均为可选。

        :param symbols: 股票代码列表。如果为 None 或空列表，则查询所有股票。
        :param start_date: 开始日期 (e.g., '2023-01-01')。如果为 None，则不限制开始日期。
        :param end_date: 结束日期 (e.g., '2023-12-31')。如果为 None，则不限制结束日期。
        :return: 包含查询结果的 Pandas DataFrame。
        """
        # 1. 构建WHERE子句的各个条件
        conditions = [f"symbol = '{symbol}'"]

        if start_date:
            conditions.append(f"date >= '{start_date}'")

        if end_date:
            conditions.append(f"date <= '{end_date}'")

        # 2. 用 " AND " 将所有条件组合起来，并在最前面加上 "WHERE"
        where_clause = f"WHERE {' AND '.join(conditions)}"
        # 3. 构建完整的SQL查询
        query = f"""
        PIVOT (
            SELECT date, symbol, indicator, value
            FROM {self.table_name}
            {where_clause}
        )
        ON indicator
        USING FIRST(value)
        ORDER BY symbol, date;
        """

        return self.query_df(query)


indicator = Indicator()

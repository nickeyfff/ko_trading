import os
import tempfile

import pandas as pd

from common import download_file
from database.base import DuckDBBase


# 基于 DuckDBBase 的 IndexTable 类
class Index(DuckDBBase):
    def __init__(self):
        super().__init__()
        self.table_name = "raw_index_constituent"
        self._create_index_table()

    def _create_index_table(self):
        """创建 index_table 表"""
        columns = {
            "index_name": "varchar",
            "name": "varchar",
            "symbol": "varchar",
        }
        self.create_table(self.table_name, columns)


class CSIndex(Index):
    def query(self, csi_name=None) -> pd.DataFrame:
        """查询 index_table 表，返回 DataFrame"""
        sql = f"SELECT * FROM {self.table_name}"
        if csi_name:
            sql += f" WHERE index_name = '{csi_name}'"

        return self.query_df(sql)

    def store_xls(self, xls_file: str):
        """解析 Excel 文件并导入数据到 csi 表"""
        # Excel 列名到数据库列名的映射
        xls_column_mapping = {
            "指数英文名称Index Name(Eng)": "index_name",
            "成份券代码Constituent Code": "code",
            "成份券名称Constituent Name": "name",
            "交易所英文名称Exchange(Eng)": "exchange",
        }
        # 交易所名称到简写映射
        exchange_mapping = {
            "Shenzhen Stock Exchange": "SZ",
            "Shanghai Stock Exchange": "SH",
            "Beijing Stock Exchange": "BJ",
        }

        try:
            # 读取 Excel 文件
            df = pd.read_excel(xls_file, dtype=str)
            # 保留需要的列
            columns_to_keep = list(xls_column_mapping.keys())
            data = df[columns_to_keep].copy()
            # 重命名列
            data.rename(columns=xls_column_mapping, inplace=True)
            # 替换交易所名称
            data["exchange"] = data["exchange"].replace(exchange_mapping)
            # 去除 index_name 中的空格
            data["index_name"] = data["index_name"].str.replace(" ", "", regex=True)
            # 生成 symbol 列（code.exchange）
            data["symbol"] = (data["exchange"].str.lower()).str.cat(data["code"])

            # 避免重复数据
            conditions = {"index_name": data["index_name"].iloc[0]}
            self.delete(self.table_name, conditions)

            column_order = ["index_name", "name", "symbol"]
            data = data[column_order].copy()
            # 插入 DataFrame 数据
            self.insert_dataframe(self.table_name, data)
        except Exception as e:
            raise e


csindex = CSIndex()


def import_csindex(index_file_name: str, temp_dir: str) -> str:
    """
    下载并处理单个中证指数文件。
    """
    csindex_url = "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/"
    url = csindex_url + index_file_name
    output_path = os.path.join(temp_dir, index_file_name)

    if not download_file(url, output_path):
        raise Exception(f"下载 {index_file_name} 失败")

    try:
        csindex.store_xls(output_path)
    except Exception as e:
        raise Exception(f"处理文件时出错: {e}") from e

    return index_file_name


def run_csindex_update():
    print(f"\n{'=' * 50}\n开始更新指数成分信息")

    index_list = [
        {"name": "全部A股", "file": "930903cons.xls"},
        {"name": "沪深300", "file": "000300cons.xls"},
        {"name": "中证500", "file": "000905cons.xls"},
        {"name": "中证1000", "file": "000852cons.xls"},
        {"name": "中证2000", "file": "932000cons.xls"},
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        for index in index_list:
            r = import_csindex(index_file_name=index["file"], temp_dir=temp_dir)
            print(f"✅ {index['name'] + ':'} {r} 处理成功")

    print(f"🎉 指数成分更新完成\n{'=' * 50}\n")

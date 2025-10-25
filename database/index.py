import os
import tempfile

import pandas as pd

from database.base import DuckDBBase
from utils import download_file, get_logger

logger = get_logger(__name__)


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
            "code": "varchar",
            "exchange": "varchar",
        }
        self.create_table(self.table_name, columns)


class CSI(Index):
    def query(self, csi_name) -> pd.DataFrame:
        """查询 index_table 表，返回 DataFrame"""
        conditions = {"index_name": csi_name}
        return self.select(table_name=self.table_name, conditions=conditions)

    def store_csi_xls(self, xls_file: str):
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

            # 插入 DataFrame 数据
            inserted_rows = self.insert_dataframe(self.table_name, data)
            logger.info(
                f"从 {xls_file} 导入 {inserted_rows} 条数据到 {self.table_name} 表"
            )
        except Exception as e:
            logger.error(f"导入 {xls_file} 失败: {e}")
            raise


class GGT(Index):
    def query(self) -> pd.DataFrame:
        """查询 index_table 表，返回 DataFrame"""
        conditions = {"index_name": "GGT"}
        return self.select(table_name=self.table_name, conditions=conditions)

    def store_ggt_xls(self, xls_file: str):
        """解析 Excel 文件并导入数据到 ggt 表"""
        xls_column_mapping = {
            "证券代码": "code",
            "英文简称": "en_name",
            "中文简称(参考)": "name",
            "品种": "type",
        }

        try:
            # 读取 Excel 文件
            df = pd.read_excel(xls_file, dtype=str)
            # 保留需要的列
            columns_to_keep = list(xls_column_mapping.keys())
            data = df[columns_to_keep].copy()

            # 重命名列
            data.rename(columns=xls_column_mapping, inplace=True)
            # 过滤股票类型
            data = data[data["type"] == "股票"]

            data["index_name"] = "GGT"
            # 生成 symbol 列（code.exchange）
            data["symbol"] = data["code"] + ".HK"
            data["exchange"] = "HK"

            # 删除 type 列
            data.drop(columns=["type", "en_name"], inplace=True)

            # 避免重复数据
            conditions = {"index_name": "GGT"}
            self.delete(self.table_name, conditions)

            # 插入 DataFrame 数据
            inserted_rows = self.insert_dataframe(self.table_name, data)
            logger.info(
                f"从 {xls_file} 导入 {inserted_rows} 条数据到 {self.table_name} 表"
            )
        except Exception as e:
            logger.error(f"导入 {xls_file} 失败: {e}")
            raise


class SP500(Index):
    def query(self) -> pd.DataFrame:
        """查询 index_table 表，返回 DataFrame"""
        conditions = {"index_name": "SP500"}
        return self.select(table_name=self.table_name, conditions=conditions)

    def store_sp500_csv(self, csv_file: str):
        """解析 CSV 文件并导入数据到 sp500 表"""
        xls_column_mapping = {
            "Symbol": "code",
            "Security": "name",
        }
        try:
            # 读取 CSV 文件
            data = pd.read_csv(csv_file, dtype=str, usecols=["Symbol", "Security"])

            data.rename(columns=xls_column_mapping, inplace=True)
            # 去除 index_name 中的空格
            data["index_name"] = "SP500"
            # 生成 symbol 列（code.exchange）
            data["symbol"] = data["code"] + ".US"
            data["exchange"] = "US"

            # 避免重复数据
            conditions = {"index_name": "SP500"}
            self.delete(self.table_name, conditions)

            # 插入 DataFrame 数据
            inserted_rows = self.insert_dataframe(self.table_name, data)
            logger.info(
                f"从 {csv_file} 导入 {inserted_rows} 条数据到 {self.table_name} 表"
            )
        except Exception as e:
            logger.error(f"导入 {csv_file} 失败: {e}")
            raise


csi = CSI()
ggt = GGT()
sp500 = SP500()


def update_index():
    """Update CSI, SP500, and GGT IndexTable by downloading and processing index files."""

    with tempfile.TemporaryDirectory() as temp_dir:
        os.makedirs(temp_dir, exist_ok=True)

        # Unified configuration for all index files
        index_configs = [
            {
                "name": "ChinaA",
                "url": "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/930903cons.xls",
                "output_file": "cn.xls",
                "processor": csi.store_csi_xls,
                "headers": None,
            },
            {
                "name": "沪深300",
                "url": "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/000300cons.xls",
                "output_file": "cn300.xls",
                "processor": csi.store_csi_xls,
                "headers": None,
            },
            {
                "name": "中证500",
                "url": "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/000905cons.xls",
                "output_file": "cn500.xls",
                "processor": csi.store_csi_xls,
                "headers": None,
            },
            {
                "name": "中证1000",
                "url": "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/000852cons.xls",
                "output_file": "cn1000.xls",
                "processor": csi.store_csi_xls,
                "headers": None,
            },
            {
                "name": "标普500",
                "url": "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/refs/heads/main/data/constituents.csv",
                "output_file": "sp500.csv",
                "processor": sp500.store_sp500_csv,
                "headers": None,
            },
            {
                "name": "港股通",
                "url": "https://query.sse.com.cn/commonExcelDd.do?sqlId=COMMON_SSE_JYFW_HGT_XXPL_BDZQQD_L&keyword=",
                "output_file": "ggt.xls",
                "headers": {
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Referer": "https://www.sse.com.cn/services/hkexsc/disclo/eligible/",
                },
                "processor": ggt.store_ggt_xls,
            },
        ]

        # Download and process index file sequentially
        for config in index_configs:
            output_path = os.path.join(temp_dir, config["output_file"])
            if download_file(config["url"], output_path, headers=config["headers"]):
                try:
                    config["processor"](output_path)
                except Exception as e:
                    print(f"Error processing {config['name']}: {e}")
            else:
                print(f"Failed to download {config['name']}")


if __name__ == "__main__":
    update_index()

# KO Trading

提供基础的 Python 代码，用于使用 [tdx2db](https://github.com/jing2uo/tdx2db) 处理后 DuckDB 中的数据，可以：

- 查询股票前复权、换手率和日线数据
- 更新和查询申万行业分类信息
- 更新和查询指数成分股数据
- 批量计算技术指标并导入 Duckdb
- 体验 Qlib 量化平台功能

## 开始使用

### 前提条件

- Python 3.12 （Qlib 最高支持 3.12）
- uv，脚本和文档里都用的它
- 使用 `tdx2db` 转换生成的 DuckDB 数据库
- 安装 `req.txt` 中列出的依赖项：
  ```bash
  uv pip install -r req.txt
  ```

### 使用方法

在 vscode 下开发，依赖 python 和 jupyter 插件，使用 vscode 调试跑起来的坑可能不多~

1. **设置数据库**：确保 `tdx2db` 转换生成的 DuckDB 数据库可用
2. **配置环境变量**：修改`.env` 中的 DBPATH 变量，请自行确认编辑器会正确读取 `.env` ，也可以使用全局变量
3. **执行示例**：运行 `example.ipynb` 中的示例代码理解工作流程

### Qlib 体验

1.  使用 tdx2db 处理好日线数据和复权因子
2.  使用 export_for_qlib 导出 csv
3.  配置 qlib_dump.sh 中的变量
4.  TDX_EXPORT 表示 export_for_qlib 的 output 目录
5.  QLIB_PROVIDER_URI 表示 qlib 数据目录
6.  执行 qlib_dump.sh init 初始化
7.  uv run qlib_test.py 就能看到加载数据、训练和回测过程

输出丢给 ai 让它解释，然后慢慢研究吧。

可以参考 docs 下华泰的文档(过时严重但有个完整流程可以对照)，还有 [Qlib 官方文档](https://qlib.readthedocs.io/en/latest/introduction/introduction.html)。

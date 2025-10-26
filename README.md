# Quant Base

此仓库提供基础的 Python 代码，用于使用 [tdx2db](https://github.com/jing2uo/tdx2db) 处理后 DuckDB 中的数据，可以：

- 查询股票数据
- 更新和查询申万行业分类信息
- 更新和查询指数成分股数据
- 体验 Qlib 量化平台功能

## 项目结构

```
├── database
│   ├── base.py         # 数据库核心操作
│   ├── index.py        # 指数成分股数据处理
│   ├── shenwan.py      # 申万行业分类数据
│   └── stock.py        # 读取 tdx2db 保存的股票数据
├── example.ipynb        # 使用示例笔记本
├── export_for_qlib      # 从 duckdb 中导出 qlib 需要的 csv
├── qlib_dump.sh         # 处理上面导出的 csv 到 qlib bin
├── qlib_predict.py      # qlib 预测明天的股票情况，仅供参考流程
├── qlib_test.py         # qlib 回测，仅供验证安装
├── README.md
├── req.txt              # 依赖项
├── scripts              # 从 qlib 拿的脚本
│   ├── check_data_health.py
│   ├── check_dump_bin.py
│   ├── collect_info.py
│   ├── dump_bin.py     # 主要用这个处理 csv 转换
│   └── dump_pit.py
└── utils
    ├── download.py     # 文件下载工具
    └── log.py          # 日志记录工具
```

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
   1. TDX_EXPORT 表示 export_for_qlib 的 output 目录
   2. QLIB_PROVIDER_URI 表示 qlib 数据目录
4. 执行 qlib_dump.sh init 初始化
5.  uv run qlib_test.py 就能看到加载数据、训练和回测过程

输出丢给 ai 让它解释，然后慢慢研究吧。

可以参考 docs 下华泰的文档，也可以看官方文档：https://qlib.readthedocs.io/en/latest/introduction/introduction.html



### 项目扩展

您可以基于本仓库添加自定义模块，例如：

- 基于技术指标的选股逻辑。
- 使用复权数据执行策略回测。

# Rec-Tmall 数据处理项目

## 仓库内容

```
├── run_all.sh              # 一键运行
├── config.py               # 全局配置（路径、Spark参数、字段定义）
├── download_check.py       # 下载文件校验
├── etl.py                  # 全量 ETL 清洗
├── quality_report.py       # 质量报告 + 数据字典
├── sampling.py             # 分层抽样
├── debug.zip               # 调试样本集（~1万行，21MB）
├── quality_report.zip      # 质量报告（MD + JSON）
├── quality_report.md       # 质量报告 Markdown
├── quality_report.json     # 质量报告 JSON
├── data_dictionary.md      # 数据字典 Markdown
├── data_dictionary.json    # 数据字典 JSON
└── README.md
```

## 运行方式

### 1. 环境准备

```bash
python3 -m venv venv
source venv/bin/activate
pip install pyspark pandas pyarrow
```

需要 Java 11+（Spark 依赖）。

### 2. 下载数据集

去 https://tianchi.aliyun.com/dataset/140281 下载全部文件，放到 `data/raw/`：

```
data/raw/
├── Tianchi_2014001_Rec_Tmall_Product          # 商品属性表
├── Tianchi_2014002_Rec_Tmall_Log.tar.gz.001   # 日志分卷1
├── Tianchi_2014002_Rec_Tmall_Log.tar.gz.002   # 日志分卷2
├── Tianchi_2014002_Rec_Tmall_Log.tar.gz.003   # 日志分卷3
└── Tianchi_2014003_Rec_Tmall_Review           # 评论表
```

### 3. 一键运行

```bash
bash run_all.sh
```

依次执行：文件校验 → ETL清洗 → 质量报告 → 分层抽样

## 交付物

| 交付物 | 说明 |
|--------|------|
| `quality_report.zip` / `.md` / `.json` | 数据质量校验报告 |
| `data_dictionary.md` / `.json` | 字段数据字典 |
| `debug.zip` | 调试样本集（Parquet，可直接读取） |
| 开发样本集 | 运行 `bash run_all.sh` 后生成于 `data/samples/dev/` |
| 全量清洗数据 | 运行后生成于 `data/cleaned/`（本地存储） |

## 环境信息

- Python 3.9 + PySpark 4.0.4 + pandas 2.3.3 + PyArrow 21.0.0
- Java 11 (Zulu)
- Spark: Driver 8GB / 10核 / Shuffle 40分区

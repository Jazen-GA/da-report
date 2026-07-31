"""
Rec-Tmall 项目全局配置
所有路径、Spark参数、字段定义集中管理
"""
import os

# ── 项目根目录 ──
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── 数据目录 ──
DATA_RAW = os.path.join(PROJECT_ROOT, "data", "raw")
DATA_CLEANED = os.path.join(PROJECT_ROOT, "data", "cleaned")
DATA_SAMPLES = os.path.join(PROJECT_ROOT, "data", "samples")
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")

# ── 原始文件路径 ──
RAW_FILES = {
    "product": os.path.join(DATA_RAW, "tianchi_2014001_rec_tmall_product.txt"),
    "log_parta": os.path.join(DATA_RAW, "tianchi_2014002_rec_tmall_log_parta.txt"),
    "log_partb": os.path.join(DATA_RAW, "tianchi_2014002_rec_tmall_log_partb.txt"),
    "log_partc": os.path.join(DATA_RAW, "tianchi_2014002_rec_tmall_log_partc.txt"),
    # Spark 用 glob 读取全部三部分
    "log_glob": os.path.join(DATA_RAW, "tianchi_2014002_rec_tmall_log_part*.txt"),
    "review": os.path.join(DATA_RAW, "tianchi_2014003_rec_tmall_review.txt"),
}
# 日志分卷文件列表（用于校验）
LOG_PARTS = [
    "tianchi_2014002_rec_tmall_log_parta.txt",
    "tianchi_2014002_rec_tmall_log_partb.txt",
    "tianchi_2014002_rec_tmall_log_partc.txt",
]

# ── 清洗后输出路径 ──
CLEANED_FILES = {
    "product": os.path.join(DATA_CLEANED, "product"),
    "log": os.path.join(DATA_CLEANED, "log"),
    "review": os.path.join(DATA_CLEANED, "review"),
}

# ── 样本输出路径 ──
SAMPLE_FILES = {
    "full": os.path.join(DATA_SAMPLES, "full"),
    "dev": os.path.join(DATA_SAMPLES, "dev"),
    "debug": os.path.join(DATA_SAMPLES, "debug"),
}

# ── 官方数据规模（用于交叉验证）──
OFFICIAL_COUNTS = {
    "product": 8_133_507,
    "log": 1_333_729_303,
    "review": 11_224_814,
}

# ── 字段定义 ──
PRODUCT_FIELDS = {
    "item_id": {"type": "int", "desc": "商品ID，唯一标识"},
    "title": {"type": "string", "desc": "NLP提取的关键词，空格分隔"},
    "pict_url": {"type": "string", "desc": "商品图片URL"},
    "category": {"type": "string", "desc": "类别，格式x-y"},
    "brand_id": {"type": "string", "desc": "品牌ID，如b1"},
    "seller_id": {"type": "string", "desc": "商家ID，如s1"},
}

LOG_FIELDS = {
    "item_id": {"type": "int", "desc": "商品ID"},
    "user_id": {"type": "string", "desc": "用户ID，如u9774184"},
    "action": {"type": "string", "desc": "行为类型: click/collect/cart/alipay"},
    "vtime": {"type": "string", "desc": "行为时间，格式yyyy-mm-dd hh:mm:ss"},
}

REVIEW_FIELDS = {
    "item_id": {"type": "int", "desc": "商品ID"},
    "user_id": {"type": "string", "desc": "用户ID"},
    "feedback": {"type": "string", "desc": "NLP提取的评论关键词，空格分隔"},
}

# ── 日志表分区键 ──
LOG_PARTITION_COLS = ["action_date", "action_type"]

# ── Spark 配置（32GB Mac, 10核）──
SPARK_CONFIG = {
    "spark.driver.memory": "12g",
    "spark.executor.memory": "8g",
    "spark.sql.shuffle.partitions": "500",
    "spark.default.parallelism": "10",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.driver.maxResultSize": "4g",
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.3",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "4g",
    # 输入分片：64MB → 51.8GB 拆成 ~800 个分片，并行度拉满
    "spark.sql.files.maxPartitionBytes": "67108864",   # 64MB
    "spark.sql.files.openCostInBytes": "16777216",     # 16MB
    "spark.hadoop.parquet.enable.summary-metadata": "false",
}

# ── SparkSession 统一入口（所有脚本共用，禁止各文件重复定义）──
def create_spark(app_name="Rec-Tmall"):
    """创建预配置的 SparkSession"""
    from pyspark.sql import SparkSession
    builder = SparkSession.builder.appName(app_name).master("local[*]")
    for k, v in SPARK_CONFIG.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

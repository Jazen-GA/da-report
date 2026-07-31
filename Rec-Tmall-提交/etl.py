"""
Rec-Tmall 全量 ETL 数据清洗脚本
功能: 读取原始文件 → 清洗 → 转 Parquet+Snappy 分区存储
用法: python scripts/etl.py [--table product|log|review|all]
"""
import os, sys, time, argparse, glob
from datetime import datetime
from pyspark.sql import functions as F, types as T
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.config import *

# ── 定义各表 Schema ──
PRODUCT_SCHEMA = T.StructType([
    T.StructField("item_id", T.IntegerType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("pict_url", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("brand_id", T.StringType(), True),
    T.StructField("seller_id", T.StringType(), True),
])

LOG_SCHEMA = T.StructType([
    T.StructField("item_id", T.IntegerType(), True),
    T.StructField("user_id", T.StringType(), True),
    T.StructField("action", T.StringType(), True),
    T.StructField("vtime", T.StringType(), True),
])

REVIEW_SCHEMA = T.StructType([
    T.StructField("item_id", T.IntegerType(), True),
    T.StructField("user_id", T.StringType(), True),
    T.StructField("feedback", T.StringType(), True),
])

def read_raw(spark, path, schema, table_name):
    """
    读取原始文件（SOH分隔符 ）
    """
    print(f"  [{table_name}] 读取原始文件: {path}")
    t0 = time.time()

    if "*" in path or "?" in path:
        matches = glob.glob(path)
        if not matches:
            raise FileNotFoundError(f"glob 未匹配到任何文件: {path}")
    elif not os.path.exists(path):
        raise FileNotFoundError(f"文件不存在: {path}")

    df = spark.read \
        .option("delimiter", "") \
        .option("header", "false") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .option("mode", "PERMISSIVE") \
        .option("ignoreTrailingWhiteSpace", "true") \
        .option("ignoreLeadingWhiteSpace", "true") \
        .schema(schema) \
        .csv(path)

    elapsed = time.time() - t0
    count = df.count()
    print(f"  [{table_name}] 读取完成: {count:,} 行, 耗时 {elapsed:.1f}s")
    return df

def clean_product(spark):
    """清洗商品属性表"""
    print("\n" + "=" * 60)
    print("1/3 清洗商品属性表 (Product)")
    print("=" * 60)

    path = RAW_FILES["product"]
    df = read_raw(spark, path, PRODUCT_SCHEMA, "Product")

    initial_count = df.count()

    # 1. 去重（按 item_id）
    print("  去重...")
    df = df.dropDuplicates(["item_id"])
    dedup_count = df.count()
    print(f"    去重前: {initial_count:,}, 去重后: {dedup_count:,}, 删除: {initial_count - dedup_count:,}")

    # 2. 剔除 item_id 为空
    print("  剔除 item_id 为空...")
    null_before = df.filter(F.col("item_id").isNull()).count()
    df = df.filter(F.col("item_id").isNotNull())
    null_after = df.filter(F.col("item_id").isNull()).count()
    print(f"    剔除 item_id 为空: {null_before} 行")

    # 3. 缺失率统计
    print("  缺失率统计:")
    total = df.count()
    for field in PRODUCT_FIELDS:
        null_count = df.filter(F.col(field).isNull()).count()
        null_pct = null_count / total * 100 if total > 0 else 0
        print(f"    {field}: {null_count:,} null ({null_pct:.2f}%)")

    # 4. brand_id / seller_id 规范化（去掉可能的多余前缀）
    # 保持原样，只做 trim
    df = df.withColumn("brand_id", F.trim(F.col("brand_id"))) \
           .withColumn("seller_id", F.trim(F.col("seller_id"))) \
           .withColumn("category", F.trim(F.col("category")))

    # 5. 写入 Parquet
    print("  写入 Parquet (Snappy)...")
    t0 = time.time()
    output_path = CLEANED_FILES["product"]
    df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_path)
    elapsed = time.time() - t0

    # 验证写入
    verify = spark.read.parquet(output_path).count()
    print(f"  写入完成: {verify:,} 行, 耗时 {elapsed:.1f}s")
    print(f"  输出路径: {output_path}")

    return df

def clean_log(spark):
    """清洗用户行为日志表（优化版：减少全量扫描，提高写分区并行度）"""
    print("\n" + "=" * 60)
    print("2/3 清洗用户行为日志表 (Log)")
    print("=" * 60)

    path = RAW_FILES["log_glob"]
    df = read_raw(spark, path, LOG_SCHEMA, "Log")

    # ── 一次性聚合所有质量指标 + 时间范围（真正只扫一次全表）──
    print("  聚合一键质量统计 + 时间范围...")
    valid_actions = ["click", "collect", "cart", "alipay"]
    t0 = time.time()
    agg = df.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("item_id").isNull(), 1).otherwise(0)).alias("null_item"),
        F.sum(F.when(F.col("user_id").isNull(), 1).otherwise(0)).alias("null_user"),
        F.sum(F.when(~F.col("action").isin(valid_actions), 1).otherwise(0)).alias("invalid_action"),
        F.sum(F.when(F.to_timestamp("vtime", "yyyy-MM-dd HH:mm:ss").isNull(), 1).otherwise(0)).alias("invalid_time"),
        F.min(F.to_date(F.to_timestamp("vtime", "yyyy-MM-dd HH:mm:ss"))).alias("min_date"),
        F.max(F.to_date(F.to_timestamp("vtime", "yyyy-MM-dd HH:mm:ss"))).alias("max_date"),
    ).collect()[0]
    print(f"    耗时 {time.time() - t0:.1f}s")
    print(f"    总行数: {agg['total']:,}")
    print(f"    item_id为空: {agg['null_item']:,}, user_id为空: {agg['null_user']:,}")
    print(f"    非法action: {agg['invalid_action']:,}, 时间格式错误: {agg['invalid_time']:,}")
    print(f"    时间范围: {agg['min_date']} ~ {agg['max_date']}")

    # ── 管道式过滤（不中间 count）──
    df = df.filter(F.col("item_id").isNotNull() & F.col("user_id").isNotNull())
    df = df.filter(F.col("action").isin(valid_actions))

    # 解析时间 + 剔除无效 + 生成分区列
    df = df.withColumn("_ts", F.to_timestamp("vtime", "yyyy-MM-dd HH:mm:ss"))
    df = df.filter(F.col("_ts").isNotNull())
    df = df.withColumn("action_date", F.to_date("_ts")) \
           .withColumn("action_type", F.col("action"))

    # ── 去重 + 写入（去重和写分区合并为一次 shuffle，避免两次全量操作）──
    print("  去重 (user_id+item_id+action+vtime) + 写入 Parquet 分区...")
    t0 = time.time()

    # 开大 shuffle 并行度，避免单分区过大
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    output_path = CLEANED_FILES["log"]
    df = df.dropDuplicates(["user_id", "item_id", "action", "vtime"])
    df.select("item_id", "user_id", "action", "vtime", "action_date", "action_type") \
      .repartition(200, "action_date", "action_type") \
      .write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .option("maxRecordsPerFile", "5000000") \
        .partitionBy(LOG_PARTITION_COLS) \
        .parquet(output_path)

    elapsed = time.time() - t0
    spark.conf.set("spark.sql.shuffle.partitions", "40")

    # 从 Parquet 验证（列存，比读原始 txt 快 10x+）
    print("  验证写入...")
    verify = spark.read.parquet(output_path).count()
    print(f"  写入完成: {verify:,} 行, 耗时 {elapsed:.1f}s")
    print(f"  输出路径: {output_path}")
    print(f"  去重前(agg): {agg['total']:,}, 去重后: {verify:,}, 删除: {agg['total'] - verify:,}")

def clean_review(spark):
    """清洗用户评论表"""
    print("\n" + "=" * 60)
    print("3/3 清洗用户评论表 (Review)")
    print("=" * 60)

    path = RAW_FILES["review"]
    df = read_raw(spark, path, REVIEW_SCHEMA, "Review")

    initial_count = df.count()

    # 1. 剔除核心 ID 为空
    print("  剔除 item_id/user_id 为空...")
    null_item = df.filter(F.col("item_id").isNull()).count()
    null_user = df.filter(F.col("user_id").isNull()).count()
    df = df.filter(F.col("item_id").isNotNull() & F.col("user_id").isNotNull())
    after_null = df.count()
    print(f"    item_id为空: {null_item:,}, user_id为空: {null_user:,}")
    print(f"    剔除后: {after_null:,}")

    # 2. 去重（user_id + item_id + feedback）
    print("  去重...")
    df_dedup = df.dropDuplicates(["user_id", "item_id", "feedback"])
    dedup_count = df_dedup.count()
    print(f"    去重前: {after_null:,}, 去重后: {dedup_count:,}")

    # 3. 缺失率统计
    print("  缺失率统计:")
    total = dedup_count
    for field in REVIEW_FIELDS:
        null_count = df_dedup.filter(F.col(field).isNull()).count()
        null_pct = null_count / total * 100 if total > 0 else 0
        print(f"    {field}: {null_count:,} null ({null_pct:.2f}%)")

    # 4. 写入 Parquet
    print("  写入 Parquet (Snappy)...")
    t0 = time.time()
    output_path = CLEANED_FILES["review"]
    df_dedup.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_path)
    elapsed = time.time() - t0

    verify = spark.read.parquet(output_path).count()
    print(f"  写入完成: {verify:,} 行, 耗时 {elapsed:.1f}s")
    print(f"  输出路径: {output_path}")

    return df_dedup

def main():
    parser = argparse.ArgumentParser(description="Rec-Tmall ETL")
    parser.add_argument("--table", choices=["product", "log", "review", "all"], default="all")
    args = parser.parse_args()

    spark = create_spark()

    print("=" * 60)
    print(f"Rec-Tmall ETL Pipeline — {args.table}")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    total_start = time.time()

    try:
        if args.table in ("product", "all"):
            clean_product(spark)
        if args.table in ("log", "all"):
            clean_log(spark)
        if args.table in ("review", "all"):
            clean_review(spark)
    finally:
        spark.stop()

    total_elapsed = time.time() - total_start
    print("\n" + "=" * 60)
    print(f"ETL 全部完成! 总耗时: {total_elapsed/60:.1f} 分钟")
    print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

if __name__ == "__main__":
    main()

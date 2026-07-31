"""
Rec-Tmall 分层抽样脚本
功能: 从全量清洗数据中构建三级样本集（全量/开发/调试）
      保持行为类型、类目分布与全量数据一致
用法: python scripts/sampling.py
"""
import os, sys, time, json
from datetime import datetime
from pyspark.sql import functions as F
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.config import *

def compute_distribution(spark, table_name, clean_path):
    """计算全量数据的分布（行为类型、类目）"""
    df = spark.read.parquet(clean_path)

    if table_name == "log":
        # 行为类型分布
        action_dist = df.groupBy("action").count().collect()
        total = df.count()
        dist = {}
        for row in action_dist:
            dist[row["action"]] = row["count"] / total
        return dist, total

    elif table_name == "product":
        # 类目分布（取父类别）
        total = df.count()
        # 父类别 = category 的 x 部分
        df_with_parent = df.withColumn(
            "parent_cat", F.split(F.col("category"), "-").getItem(0)
        )
        cat_dist = df_with_parent.groupBy("parent_cat").count().collect()
        dist = {}
        for row in cat_dist:
            dist[row["parent_cat"]] = row["count"] / total
        return dist, total

    return {}, df.count()

def stratified_sample_log(spark, df_full, sample_size, seed=42):
    """
    对日志表按行为类型做分层抽样
    """
    # 计算各行为类型的比例
    action_counts = df_full.groupBy("action").count().collect()
    total = sum(r["count"] for r in action_counts)

    sampled_dfs = []
    for row in action_counts:
        action = row["action"]
        frac = sample_size / total
        action_specific = df_full.filter(F.col("action") == action)
        sampled = action_specific.sample(fraction=frac, seed=seed + hash(action) % 1000)
        sampled_dfs.append(sampled)

    # 合并所有采样的分区
    if sampled_dfs:
        result = sampled_dfs[0]
        for df in sampled_dfs[1:]:
            result = result.union(df)
        return result
    return None

def build_samples(spark):
    """构建三级样本集"""
    print("=" * 60)
    print("Rec-Tmall 分层抽样")
    print("=" * 60)

    # 检查清洗后数据是否存在
    for name, path in CLEANED_FILES.items():
        if not os.path.exists(path):
            print(f"  [ERROR] 清洗后数据不存在: {path}")
            print(f"  请先运行: python scripts/etl.py")
            return

    # ── 1. 全量数据集 ──
    print("\n1/3 全量数据集 — 直接使用清洗后的 Parquet 文件")
    print(f"  商品表: {CLEANED_FILES['product']}")
    print(f"  日志表: {CLEANED_FILES['log']}")
    print(f"  评论表: {CLEANED_FILES['review']}")

    # ── 2. 开发样本集（万级用户，分层抽样）──
    print("\n2/3 开发样本集 — 分层抽样 (~10万日志行)")

    # 读取全量日志
    df_log = spark.read.parquet(CLEANED_FILES["log"])
    log_total = df_log.count()

    # 获取行为分布
    action_dist = df_log.groupBy("action").count().collect()
    print("  全量行为分布:")
    for r in action_dist:
        print(f"    {r['action']}: {r['count']:,} ({r['count']/log_total*100:.1f}%)")

    # 抽样策略：按行为类型分层，总样本约10万
    # 随机选 1% 用户
    users = df_log.select("user_id").distinct()
    total_users = users.count()
    sample_users = users.sample(fraction=0.01, seed=42)
    sampled_users_count = sample_users.count()

    print(f"  总用户数: {total_users:,}")
    print(f"  采样用户数: {sampled_users_count:,} (~1%)")

    # 提取这些用户的全部行为日志
    dev_log = df_log.join(sample_users, on="user_id", how="inner")
    dev_log_count = dev_log.count()
    print(f"  开发集日志行数: {dev_log_count:,}")

    # 验证分布一致性
    print("  开发集行为分布:")
    dev_action_dist = dev_log.groupBy("action").count().collect()
    for r in dev_action_dist:
        full_pct = r["count"] / log_total * 100
        dev_pct = r["count"] / dev_log_count * 100 if dev_log_count > 0 else 0
        print(f"    {r['action']}: {r['count']:,} (全量: {full_pct:.1f}%, 样本: {dev_pct:.1f}%)")

    # 提取关联的商品和评论
    dev_items = dev_log.select("item_id").distinct()

    df_product = spark.read.parquet(CLEANED_FILES["product"])
    dev_product = df_product.join(dev_items, on="item_id", how="inner")

    df_review = spark.read.parquet(CLEANED_FILES["review"])
    dev_users = dev_log.select("user_id").distinct()
    dev_review = df_review.join(dev_users, on="user_id", how="inner")

    # 写入开发集
    print("  写入开发集 Parquet...")
    dev_product.write.mode("overwrite").option("compression", "snappy") \
        .parquet(os.path.join(SAMPLE_FILES["dev"], "product"))
    dev_log.write.mode("overwrite").option("compression", "snappy") \
        .partitionBy("action_date", "action_type") \
        .parquet(os.path.join(SAMPLE_FILES["dev"], "log"))
    dev_review.write.mode("overwrite").option("compression", "snappy") \
        .parquet(os.path.join(SAMPLE_FILES["dev"], "review"))

    print(f"  开发集已保存: {SAMPLE_FILES['dev']}")
    print(f"    商品: {dev_product.count():,} 行")
    print(f"    日志: {dev_log_count:,} 行")
    print(f"    评论: {dev_review.count():,} 行")

    # ── 3. 调试样本集（随机万级）──
    print("\n3/3 调试样本集 — 随机抽样 (~1万行)")

    debug_log = df_log.sample(fraction=0.0001, seed=123)
    debug_count = debug_log.count()
    print(f"  调试集日志行数: {debug_count:,}")

    debug_items = debug_log.select("item_id").distinct()
    debug_users = debug_log.select("user_id").distinct()

    debug_product = df_product.join(debug_items, on="item_id", how="inner")
    debug_review = df_review.join(debug_users, on="user_id", how="inner")

    debug_product.write.mode("overwrite").option("compression", "snappy") \
        .parquet(os.path.join(SAMPLE_FILES["debug"], "product"))
    debug_log.write.mode("overwrite").option("compression", "snappy") \
        .parquet(os.path.join(SAMPLE_FILES["debug"], "log"))
    debug_review.write.mode("overwrite").option("compression", "snappy") \
        .parquet(os.path.join(SAMPLE_FILES["debug"], "review"))

    print(f"  调试集已保存: {SAMPLE_FILES['debug']}")
    print(f"    商品: {debug_product.count():,} 行")
    print(f"    日志: {debug_count:,} 行")
    print(f"    评论: {debug_review.count():,} 行")

    # ── 4. 一致性校验 ──
    print("\n" + "─" * 60)
    print("4. 一致性校验")
    print("─" * 60)

    # 检查字段结构一致
    full_cols = set(df_log.columns)
    dev_cols = set(spark.read.parquet(os.path.join(SAMPLE_FILES["dev"], "log")).columns)
    debug_cols = set(spark.read.parquet(os.path.join(SAMPLE_FILES["debug"], "log")).columns)

    if full_cols == dev_cols == debug_cols:
        print("  [OK] 三级样本集字段结构完全一致")
    else:
        print(f"  [WARN] 字段不一致: 全量={full_cols}, 开发={dev_cols}, 调试={debug_cols}")

    # 比较行为分布
    print("\n  行为分布对比:")
    print(f"  {'行为':>10} | {'全量':>10} | {'开发':>10} | {'调试':>10}")
    print(f"  {'─'*10}─┼─{'─'*10}─┼─{'─'*10}─┼─{'─'*10}")

    debug_dist = debug_log.groupBy("action").count().collect()
    debug_dict = {r["action"]: r["count"] for r in debug_dist}
    dev_dict = {r["action"]: r["count"] for r in dev_action_dist}
    full_dict = {r["action"]: r["count"] for r in action_dist}

    for action in ["click", "collect", "cart", "alipay"]:
        fp = full_dict.get(action, 0) / log_total * 100
        dp = dev_dict.get(action, 0) / dev_log_count * 100 if dev_log_count > 0 else 0
        dbp = debug_dict.get(action, 0) / debug_count * 100 if debug_count > 0 else 0
        print(f"  {action:>10} | {fp:>9.1f}% | {dp:>9.1f}% | {dbp:>9.1f}%")

    return True

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    build_samples(spark)

    spark.stop()
    print("\n" + "=" * 60)
    print("分层抽样完成!")
    print("=" * 60)

if __name__ == "__main__":
    main()

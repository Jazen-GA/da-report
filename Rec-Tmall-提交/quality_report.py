"""
Rec-Tmall 元数据校验 + 数据质量报告 + 数据字典 一体化脚本
功能: 字段校验、行数交叉验证、缺失率/重复率/异常值统计、行为分布、数据字典、质量报告(MD+JSON)
用法: python scripts/quality_report.py
"""
import os, sys, json
from datetime import datetime
from pyspark.sql import functions as F
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.config import *

# ── 字段与官方定义的1:1对比 ──
TABLE_FIELD_MAP = {
    "product": (PRODUCT_FIELDS, ["item_id", "title", "pict_url", "category", "brand_id", "seller_id"]),
    "log": (LOG_FIELDS, ["item_id", "user_id", "action", "vtime"]),
    "review": (REVIEW_FIELDS, ["item_id", "user_id", "feedback"]),
}

def validate_fields(df, table_name):
    """与官方字段定义做1:1比对"""
    expected_fields, expected_order = TABLE_FIELD_MAP[table_name]
    actual_cols = list(df.columns)
    # 排除分区字段
    actual_core = [c for c in actual_cols if c not in ("action_date", "action_type")]

    result = {
        "expected": list(expected_fields.keys()),
        "actual_core": actual_core,
        "missing": [f for f in expected_fields if f not in actual_core],
        "extra": [f for f in actual_core if f not in expected_fields],
        "match": sorted(expected_fields.keys()) == sorted(actual_core),
    }
    return result

def analyze_table(spark, clean_path, table_name):
    """对单张表做全面质量分析"""
    if not os.path.exists(clean_path):
        return {"table": table_name, "error": "清洗后数据不存在"}

    df = spark.read.parquet(clean_path)
    total = df.count()
    official = OFFICIAL_COUNTS[table_name]

    # 字段校验
    field_check = validate_fields(df, table_name)

    # 字段详情
    fields_info = {}
    field_defs = TABLE_FIELD_MAP[table_name][0]

    for fname, finfo in field_defs.items():
        if fname not in df.columns:
            fields_info[fname] = {"error": "字段不存在"}
            continue

        null_count = df.filter(F.col(fname).isNull()).count()
        distinct_count = df.select(fname).distinct().count()

        finfo_result = {
            "null_count": null_count,
            "null_rate": round(null_count / total * 100, 4) if total > 0 else 0,
            "distinct_count": distinct_count,
        }

        # 类型特定分析
        if finfo.get("type") == "int":
            row = df.select(F.min(fname), F.max(fname)).first()
            finfo_result["min"] = row[0]
            finfo_result["max"] = row[1]

        if fname == "action":
            for a in ["click", "collect", "cart", "alipay"]:
                cnt = df.filter(F.col(fname) == a).count()
                finfo_result[a] = cnt
                finfo_result[f"{a}_pct"] = round(cnt / total * 100, 2) if total > 0 else 0
            finfo_result["invalid"] = df.filter(~F.col(fname).isin(["click", "collect", "cart", "alipay"])).count()

        if fname == "vtime":
            min_v, max_v = df.select(F.min(fname), F.max(fname)).first()
            finfo_result["min_time"] = str(min_v)
            finfo_result["max_time"] = str(max_v)
            finfo_result["invalid_format"] = df.filter(
                F.to_timestamp(fname, "yyyy-MM-dd HH:mm:ss").isNull() & F.col(fname).isNotNull()
            ).count()

        if fname == "category":
            finfo_result["top10"] = [
                {"category": r["category"], "count": r["count"]}
                for r in df.groupBy("category").count().orderBy(F.desc("count")).limit(10).collect()
            ]

        if fname == "brand_id":
            finfo_result["sample_values"] = [
                r["brand_id"] for r in df.select(fname).filter(F.col(fname).isNotNull()).limit(5).collect()
            ]

        fields_info[fname] = finfo_result

    return {
        "table": table_name,
        "row_count": total,
        "official_count": official,
        "row_diff": total - official,
        "row_diff_pct": round(abs(total - official) / official * 100, 4),
        "field_validation": field_check,
        "fields": fields_info,
    }

def generate_data_dictionary(all_results):
    """生成数据字典 JSON + MD"""
    dd = {
        "version": "1.0",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dataset": "Rec-Tmall",
        "tables": {},
    }

    table_defs = {
        "product": (PRODUCT_FIELDS, "商品属性表"),
        "log": (LOG_FIELDS, "用户行为日志表"),
        "review": (REVIEW_FIELDS, "用户评论表"),
    }

    for tname, (fields, desc) in table_defs.items():
        # 从分析结果获取实际行数和 null 率
        result = next((r for r in all_results if r["table"] == tname), None)
        row_count = result["row_count"] if result and "row_count" in result else None

        dd["tables"][tname] = {
            "description": desc,
            "row_count": row_count,
            "fields": [],
        }
        for fname, finfo in fields.items():
            entry = {
                "name": fname,
                "type": finfo["type"],
                "description": finfo["desc"],
            }
            if result and "fields" in result and fname in result["fields"]:
                fr = result["fields"][fname]
                if "null_rate" in fr:
                    entry["null_rate"] = f"{fr['null_rate']:.2f}%"
            dd["tables"][tname]["fields"].append(entry)

    # 额外分区字段
    dd["tables"]["log"]["fields"].append({
        "name": "action_date", "type": "date",
        "description": "行为日期（分区字段，从vtime解析）",
    })
    dd["tables"]["log"]["fields"].append({
        "name": "action_type", "type": "string",
        "description": "行为类型（分区字段，同action）",
    })

    # 写入文件
    json_path = os.path.join(REPORTS_DIR, "data_dictionary.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(dd, f, ensure_ascii=False, indent=2)

    md_path = os.path.join(REPORTS_DIR, "data_dictionary.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# Rec-Tmall 数据字典\n\n")
        f.write(f"> 生成时间: {dd['generated_at']}\n\n")
        for tname, tinfo in dd["tables"].items():
            f.write(f"## {tinfo['description']} ({tname})\n\n")
            if tinfo["row_count"]:
                f.write(f"行数: {tinfo['row_count']:,}\n\n")
            f.write("| 字段 | 类型 | 说明 | 缺失率 |\n")
            f.write("|------|------|------|--------|\n")
            for field in tinfo["fields"]:
                nr = field.get("null_rate", "-")
                f.write(f"| {field['name']} | {field['type']} | {field['description']} | {nr} |\n")
            f.write("\n")

    print(f"  数据字典: {json_path}")
    print(f"            {md_path}")
    return dd

def generate_report(all_results):
    """生成完整质量报告 MD"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "# Rec-Tmall 数据质量校验报告\n",
        f"> 生成时间: {now}",
        f"> 数据集: https://tianchi.aliyun.com/dataset/140281\n",
    ]

    # ── 规模总览 ──
    lines.append("## 一、数据规模总览\n")
    lines.append("| 表名 | 官方行数 | 清洗后行数 | 差异 | 差异率 | 字段一致性 |")
    lines.append("|------|---------|-----------|------|--------|-----------|")
    for r in all_results:
        if "error" in r:
            lines.append(f"| {r['table']} | - | - | - | - | {r['error']} |")
            continue
        diff = f"{r['row_diff']:+,}"
        fc = r.get("field_validation", {})
        fm = "✅" if fc.get("match") else f"❌ 缺:{fc.get('missing',[])}"
        lines.append(f"| {r['table']} | {r['official_count']:,} | {r['row_count']:,} | {diff} | {r['row_diff_pct']:.2f}% | {fm} |")
    lines.append("")

    # ── 各表详情 ──
    table_names = {"product": "商品属性表 (Product)", "log": "用户行为日志 (Log)", "review": "用户评论表 (Review)"}
    section_nums = {"product": "二", "log": "三", "review": "四"}
    for r in all_results:
        if "error" in r:
            lines.append(f"## {r['table']}\n\n**错误**: {r['error']}\n")
            continue

        tlabel = table_names.get(r["table"], r["table"])
        sec_num = section_nums.get(r["table"], "?")
        lines.append(f"## {sec_num}、{tlabel}\n")
        lines.append(f"- 总行数: {r['row_count']:,} | 官方: {r['official_count']:,} | 差异: {r['row_diff']:+,} ({r['row_diff_pct']:.2f}%)\n")

        # 字段校验
        fc = r.get("field_validation", {})
        if fc.get("missing"):
            lines.append(f"- ⚠️ 缺少字段: {fc['missing']}\n")
        if fc.get("extra"):
            lines.append(f"- ℹ️ 额外字段: {fc['extra']}\n")

        # 缺失率
        lines.append("\n### 字段缺失率\n")
        lines.append("| 字段 | 缺失数 | 缺失率 | 去重值数 |")
        lines.append("|------|--------|--------|----------|")
        for fn, fi in r["fields"].items():
            if "error" in fi:
                lines.append(f"| {fn} | - | - | {fi['error']} |")
                continue
            lines.append(f"| {fn} | {fi['null_count']:,} | {fi['null_rate']:.4f}% | {fi.get('distinct_count', 'N/A'):,} |")
        lines.append("")

        # 特殊信息
        for fn, fi in r["fields"].items():
            if "min" in fi:
                lines.append(f"**{fn}** 范围: {fi['min']:,} ~ {fi['max']:,}\n")
            if "min_time" in fi:
                lines.append(f"**{fn}** 时间范围: {fi['min_time']} ~ {fi['max_time']}, 格式错误: {fi.get('invalid_format', 0):,}\n")
            if "click" in fi:
                lines.append("**行为分布**:\n")
                lines.append("| 行为 | 数量 | 占比 |")
                lines.append("|------|------|------|")
                for a in ["click", "collect", "cart", "alipay"]:
                    lines.append(f"| {a} | {fi.get(a, 0):,} | {fi.get(f'{a}_pct', 0):.2f}% |")
                if fi.get("invalid", 0) > 0:
                    lines.append(f"| ⚠️ 非法值 | {fi['invalid']:,} | - |")
                lines.append("")
            if "top10" in fi:
                lines.append("**类别 Top10**:\n")
                for item in fi["top10"][:5]:
                    lines.append(f"- {item['category']}: {item['count']:,}\n")
                lines.append("")

    # ── 结论 ──
    lines.append("## 五、校验结论\n")
    all_ok = True
    for r in all_results:
        if "error" in r:
            lines.append(f"- **{r['table']}**: ❌ {r['error']}")
            all_ok = False
        elif r["row_diff_pct"] > 1.0:
            lines.append(f"- **{r['table']}**: ⚠️ 行数偏差 {r['row_diff_pct']:.2f}%")
            all_ok = False
        else:
            lines.append(f"- **{r['table']}**: ✅ 通过 (偏差 {r['row_diff_pct']:.2f}%)")

    if all_ok:
        lines.append("\n**总体结论: 所有校验通过，数据质量合格。**\n")
    else:
        lines.append("\n**总体结论: 存在需关注问题，详见上述标注。**\n")

    report = "\n".join(lines)
    md_path = os.path.join(REPORTS_DIR, "quality_report.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"  质量报告(MD): {md_path}")

    # JSON 版
    json_path = os.path.join(REPORTS_DIR, "quality_report.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)
    print(f"  质量报告(JSON): {json_path}")

    return report

def main():
    spark = create_spark("Rec-Tmall-Quality")

    print("=" * 60)
    print("Rec-Tmall 元数据校验 + 质量报告 + 数据字典")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    missing = [n for n, p in CLEANED_FILES.items() if not os.path.exists(p)]
    if missing:
        print(f"\n  [ERROR] 清洗后数据缺失: {missing}")
        print("  请先运行: python scripts/etl.py")
        spark.stop()
        return

    all_results = []
    for table in ["product", "log", "review"]:
        print(f"\n  分析 {table} 表...")
        r = analyze_table(spark, CLEANED_FILES[table], table)
        all_results.append(r)

    # 生成数据字典
    print("\n─" * 40)
    generate_data_dictionary(all_results)

    # 生成质量报告
    print("\n─" * 40)
    generate_report(all_results)

    # 终端摘要
    print("\n─" * 40)
    print("校验摘要:")
    for r in all_results:
        if "error" in r:
            print(f"  ❌ {r['table']}: {r['error']}")
        else:
            diff = r["row_diff_pct"]
            s = "✅" if diff < 1.0 else "⚠️"
            fc = r.get("field_validation", {})
            fm = "字段一致" if fc.get("match") else f"字段不一致: {fc.get('missing', [])}"
            print(f"  {s} {r['table']}: {r['row_count']:,} 行 (差异 {diff:.2f}%), {fm}")

    spark.stop()
    print("\n全部完成!")

if __name__ == "__main__":
    main()

"""
Rec-Tmall 全量数据下载校验脚本
用法: python scripts/download_check.py
"""
import os, sys, hashlib
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.config import *

def get_size_mb(path):
    if os.path.exists(path):
        return os.path.getsize(path) / (1024**2)
    return 0

def count_lines_fast(path, max_mb=50):
    """快速估算行数（采样前 max_mb MB）"""
    if not os.path.exists(path):
        return 0, 0
    size = os.path.getsize(path)
    sample_size = min(size, max_mb * 1024 * 1024)
    with open(path, 'rb') as f:
        sample = f.read(sample_size)
    lines = sample.count(b'\n')
    estimated = int(lines * (size / sample_size)) if sample_size < size else lines
    return lines, estimated

def check_first_line(path, expected_sep='\x01'):
    """检查第一行的分隔符"""
    if not os.path.exists(path):
        return False, "文件不存在"
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        line = f.readline()
    fields = line.count(expected_sep)
    return fields > 0, f"{fields+1} 个字段, 分隔符正确" if fields > 0 else "分隔符可能不对"

def main():
    print("=" * 60)
    print("Rec-Tmall 数据集下载校验")
    print("=" * 60)

    # ── 检查原始文件 ──
    checks = [
        ("商品表(txt)", RAW_FILES["product"], 1.6*1024),
        ("商品表(zip)", os.path.join(DATA_RAW, "tianchi_2014001_rec_tmall_product.zip"), 416),
        ("日志 parta(txt)", RAW_FILES["log_parta"], 15*1024),
        ("日志 parta(zip)", os.path.join(DATA_RAW, "tianchi_2014002_rec_tmall_log.parta.zip"), 4.1*1024),
        ("日志 partb(txt)", RAW_FILES["log_partb"], 18*1024),
        ("日志 partb(zip)", os.path.join(DATA_RAW, "tianchi_2014002_rec_tmall_log.partb.zip"), 4.1*1024),
        ("日志 partc(txt)", RAW_FILES["log_partc"], 17*1024),
        ("日志 partc(zip)", os.path.join(DATA_RAW, "tianchi_2014002_rec_tmall_log.partc.zip"), 3.9*1024),
        ("评论表(txt)", RAW_FILES["review"], 1.6*1024),
        ("评论表(zip)", os.path.join(DATA_RAW, "tianchi_2014003_rec_tmall_review.zip"), 582),
    ]

    all_ok = True
    for name, path, expected_mb in checks:
        exists = os.path.exists(path)
        actual_mb = get_size_mb(path)
        size_ok = abs(actual_mb - expected_mb) < expected_mb * 0.3  # 30% tolerance
        status = "✓" if (exists and size_ok) else ("⚠ 大小异常" if exists else "✗ 缺失")
        print(f"  [{status}] {name}: {actual_mb:.0f} MB (预期 ~{expected_mb:.0f} MB)")
        if not (exists and size_ok):
            all_ok = False

    print()

    # ── 检查 txt 文件分隔符 ──
    print("─" * 40)
    print("分隔符校验 (SOH / \\x01):")
    for name, path in [
        ("商品表", RAW_FILES["product"]),
        ("日志 parta", RAW_FILES["log_parta"]),
        ("评论表", RAW_FILES["review"]),
    ]:
        ok, msg = check_first_line(path)
        status = "✓" if ok else "⚠"
        print(f"  [{status}] {name}: {msg}")

    print()

    # ── 日志三部分加起来估算总行数 ──
    print("─" * 40)
    print("日志分卷行数估算 (采样 50MB/卷):")
    log_total_est = 0
    for key in ["log_parta", "log_partb", "log_partc"]:
        path = RAW_FILES[key]
        sample, est = count_lines_fast(path)
        print(f"  {os.path.basename(path)}: ~{est:,.0f} 行")
        log_total_est += est

    print(f"  日志总计(估算): ~{log_total_est:,.0f} 行 (官方: {OFFICIAL_COUNTS['log']:,})")

    # 其他表
    for name, key in [("商品表", "product"), ("评论表", "review")]:
        _, est = count_lines_fast(RAW_FILES[key])
        official = OFFICIAL_COUNTS[key]
        print(f"  {name}(估算): ~{est:,.0f} 行 (官方: {official:,})")

    print()
    print("─" * 40)

    if all_ok:
        print("[OK] 所有文件校验通过，可以运行 ETL")
        return 0
    else:
        print("[需处理] 以上标注 ⚠ 或 ✗ 的文件需要检查")
        return 1

if __name__ == "__main__":
    sys.exit(main())

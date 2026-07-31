#!/bin/bash
# Rec-Tmall 一键运行脚本
# 用法: bash run_all.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "  Rec-Tmall 数据处理流程"
echo "  开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

# 激活虚拟环境
source venv/bin/activate

# Step 1: 校验下载文件
echo ""
echo ">>> Step 1/4: 校验下载文件"
python scripts/download_check.py
echo ""

# 如果日志分卷未合并，先合并
MERGED_LOG="data/raw/Tianchi_2014002_Rec_Tmall_Log"
if [ ! -f "$MERGED_LOG" ]; then
    echo ">>> 合并日志分卷..."
    cat data/raw/Tianchi_2014002_Rec_Tmall_Log.tar.gz.001 \
        data/raw/Tianchi_2014002_Rec_Tmall_Log.tar.gz.002 \
        data/raw/Tianchi_2014002_Rec_Tmall_Log.tar.gz.003 \
        > "$MERGED_LOG" 2>/dev/null && echo "  合并完成" || echo "  分卷文件不存在，跳过合并"
fi

# Step 2: ETL 清洗
echo ""
echo ">>> Step 2/4: 全量 ETL 数据清洗"
python scripts/etl.py --table all

# Step 3: 质量报告 + 数据字典
echo ""
echo ">>> Step 3/4: 生成质量报告 + 数据字典"
python scripts/quality_report.py

# Step 4: 分层抽样
echo ""
echo ">>> Step 4/4: 构建分层样本集"
python scripts/sampling.py

echo ""
echo "========================================"
echo "  全部完成!"
echo "  结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
echo "  输出文件:"
echo "    清洗数据: data/cleaned/"
echo "    样本数据: data/samples/"
echo "    质量报告: reports/quality_report.md"
echo "    数据字典: reports/data_dictionary.md"
echo "========================================"

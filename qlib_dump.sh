#!/bin/bash

# 配置路径
TDX_EXPORT=${TDX_EXPORT:-"/tmp/aabb"}
QLIB_PROVIDER_URI=${QLIB_PROVIDER_URI:-"$HOME/Documents/qlib"}

DATA_CSV_PATH="$TDX_EXPORT/data"
FACTOR_CSV_PATH="$TDX_EXPORT/factor"
QLIB_HOME=${QLIB_PROVIDER_URI}

# 参数检查
if [ $# -lt 1 ]; then
    echo "Usage: $0 [init|update]"
    exit 1
fi

MODE=$1

run_dump() {
    local mode=$1
    local path=$2
    local fields=$3

    echo "Running $mode on $path with fields: $fields"
    uv run scripts/dump_bin.py $mode \
        --data_path "$path" \
        --qlib_dir "$QLIB_HOME" \
        --symbol_field_name symbol \
        --date_field_name date \
        --include_fields "$fields"
}

case "$MODE" in
    init)
        # 初始化
        run_dump dump_all "$DATA_CSV_PATH" "open,close,high,low,volume,amount"
        run_dump dump_all "$FACTOR_CSV_PATH" "factor"
        ;;
    update)
        # 更新
        run_dump dump_update "$DATA_CSV_PATH" "open,close,high,low,volume,amount"
        run_dump dump_all "$FACTOR_CSV_PATH" "factor"
        ;;
    *)
        echo "Unknown mode: $MODE"
        echo "Usage: $0 [init|update]"
        exit 1
        ;;
esac

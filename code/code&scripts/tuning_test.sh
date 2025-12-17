#!/bin/bash

# =================================================================
# 统一资源配置函数 (用于资源敏感度测试)
# 依赖于 dense_optimized_grid.py 脚本 (已修改为接收 N, BLOCK_SIZE, GRID_DIM)
# =================================================================
function run_experiment() {
    LOCAL_SPARK_ARGS="$1"
    SCRIPT_NAME="$2"
    N="$3"
    PARAM="$4"
    LOG_FILE="$5"
    echo "Running $SCRIPT_NAME N=$N P=$PARAM with args: $LOCAL_SPARK_ARGS" >> $LOG_FILE
    spark-submit $LOCAL_SPARK_ARGS $SCRIPT_NAME $N $PARAM >> $LOG_FILE
}

# --- 实验环境定义 ---
TEST_SCRIPT="dense_optimized_grid.py"
N_TEST=10000
DEFAULT_BLOCK=1000
GRID_DIM=2
# P_TEST 参数为 BLOCK_SIZE 和 GRID_DIM
P_TEST="$DEFAULT_BLOCK $GRID_DIM" 
LOG_FILE="results_resource_tuning.txt" 
# 默认基准配置 (2 Exec x 2 Cores = 总 4 Cores)
DEFAULT_SPARK_ARGS="--master spark://master:7077 --num-executors 2 --executor-cores 2 --executor-memory 12G --driver-memory 4G"

# =================================================================
# 日志初始化
# =================================================================
START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
echo -e "\n\n======== RESOURCE TUNING EXPERIMENT STARTED at $START_TIME ========\n" >> $LOG_FILE


# =================================================================
# 1. 基准测试 (用于比较)
# =================================================================
echo "=== 1/4: 基准测试 (N=10000, 2x2 Grid, 2 Exec x 2 Cores) ===" >> $LOG_FILE
run_experiment "$DEFAULT_SPARK_ARGS --conf spark.default.parallelism=24" $TEST_SCRIPT $N_TEST "$P_TEST" $LOG_FILE


# =================================================================
# 2. 并行度测试 (Concurrency Tuning)
# =================================================================
echo "=== 2/4: 并行度敏感度测试 (Total Cores=4) ===" >> $LOG_FILE
# C-1.1 优化并行度：与核心数一致 (4) 和适度高于核心数 (8, 12)
for p in 4 8 12
do
    PARALLEL_ARGS="$DEFAULT_SPARK_ARGS --conf spark.default.parallelism=$p"
    run_experiment "$PARALLEL_ARGS" $TEST_SCRIPT $N_TEST "$P_TEST" $LOG_FILE
done


# =================================================================
# 3. Executor 粒度测试 (Granularity Tuning)
# =================================================================
echo "=== 3/4: Executor 粒度测试 (总核心数固定为 4) ===" >> $LOG_FILE

# C-2.1 [修改] 少数大 Executor 
# 发现：原测试案例 (1 Exec, 4 Cores) 会导致任务卡住，因为集群中单个 Worker 最大只有 2 Cores。
# 记录发现：由于 Worker 节点限制 (每个 Worker 只有 2 Cores)，无法分配 1 个 4 Cores 的 Executor。
# 故此处保持使用基准配置 (2 Exec x 2 Cores) 作为粒度对比的大 Executor/中等分布代表。

# C-2.2 多数小 Executor (4 Executors, 1 Core)
echo "--- Testing: 4 Executors x 1 Core (最大化分布) ---" >> $LOG_FILE
GRANULARITY_ARGS_B="$DEFAULT_SPARK_ARGS --num-executors 4 --executor-cores 1 --conf spark.default.parallelism=24"
run_experiment "$GRANULARITY_ARGS_B" $TEST_SCRIPT $N_TEST "$P_TEST" $LOG_FILE


# =================================================================
# 4. 内存与序列化优化测试 (Memory/I/O Tuning)
# =================================================================
echo "=== 4/4: 内存与序列化优化测试 ===" >> $LOG_FILE

# C-3.1 Shuffle/Execution 内存比例调整
MEMORY_ARGS="$DEFAULT_SPARK_ARGS --conf spark.default.parallelism=24 --conf spark.memory.fraction=0.7"
run_experiment "$MEMORY_ARGS" $TEST_SCRIPT $N_TEST "$P_TEST" $LOG_FILE

# C-4.1 序列化器测试 (使用 Kryo)
KRYO_ARGS="$DEFAULT_SPARK_ARGS --conf spark.default.parallelism=24 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
run_experiment "$KRYO_ARGS" $TEST_SCRIPT $N_TEST "$P_TEST" $LOG_FILE

echo "资源配置敏感度测试已完成。请检查 $LOG_FILE 文件。"
#!/bin/bash

# =================================================================
# 统一的执行函数
# =================================================================
function run_experiment() {
    LOCAL_SPARK_ARGS="$1"
    SCRIPT_NAME="$2"
    N="$3"
    PARAM="$4"
    LOG_FILE="$5"
    
    # 特殊处理 Arrow 优化，需要额外的 conf 参数
    if [[ "$SCRIPT_NAME" == "dense_optimized_arrow.py" ]]; then
        LOCAL_SPARK_ARGS="$LOCAL_SPARK_ARGS --conf spark.sql.execution.arrow.pyspark.enabled=true"
    fi
    
    echo "Running $SCRIPT_NAME N=$N P=$PARAM with args: $LOCAL_SPARK_ARGS" >> $LOG_FILE
    spark-submit $LOCAL_SPARK_ARGS $SCRIPT_NAME $N $PARAM >> $LOG_FILE
}


# =================================================================
# 统一基准配置 (P=24 最优)
# 总核数=4, Executor=2, Parallels=24
# =================================================================
SPARK_ARGS="--master spark://master:7077 \
            --num-executors 2 \
            --executor-cores 2 \
            --executor-memory 12G \
            --driver-memory 4G \
            --conf spark.default.parallelism=24"
DEFAULT_BLOCK=1000
DEFAULT_SPARSITY=0.01

# =================================================================
# 日志处理策略：追加模式 (Append)
# =================================================================
START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
echo -e "\n\n======== FINAL EXPERIMENT RUN STARTED at $START_TIME ========\n" >> results_dense.txt
echo -e "\n\n======== FINAL EXPERIMENT RUN STARTED at $START_TIME ========\n" >> results_sparse.txt
echo -e "\n\n======== FINAL EXPERIMENT RUN STARTED at $START_TIME ========\n" >> results_systemds.txt


# =================================================================
# 1. 密集矩阵核心对比 (N=10000) - 确保 Arrow 优化被加入
# =================================================================
echo "=== 1/5: 密集矩阵核心对比实验 (N=10000) ===" >> results_dense.txt

run_experiment "$SPARK_ARGS" dense_baseline.py 10000 $DEFAULT_BLOCK results_dense.txt
run_experiment "$SPARK_ARGS" dense_optimized.py 10000 $DEFAULT_BLOCK results_dense.txt
run_experiment "$SPARK_ARGS" dense_optimized_grid.py 10000 $DEFAULT_BLOCK 2 results_dense.txt 
run_experiment "$SPARK_ARGS" systemds_runner.py 10000 1.0 results_systemds.txt
#run_experiment "$SPARK_ARGS" dense_optimized_arrow.py 10000 $DEFAULT_BLOCK results_dense.txt # <<< 修正：加入 Arrow 优化 >>>


# =================================================================
# 2. 稀疏矩阵核心对比 (N=10000, S=0.01) - 加入 CSR 优化
# =================================================================
echo "=== 2/5: 稀疏矩阵核心对比实验 (N=10000, S=0.01) ===" >> results_sparse.txt

run_experiment "$SPARK_ARGS" sparse_baseline.py 10000 $DEFAULT_SPARSITY results_sparse.txt
run_experiment "$SPARK_ARGS" sparse_optimized.py 10000 $DEFAULT_SPARSITY results_sparse.txt
run_experiment "$SPARK_ARGS" sparse_optimized_df.py 10000 $DEFAULT_SPARSITY results_sparse.txt
run_experiment "$SPARK_ARGS" systemds_runner.py 10000 $DEFAULT_SPARSITY results_systemds.txt
run_experiment "$SPARK_ARGS" sparse_optimized_csr.py 10000 $DEFAULT_SPARSITY results_sparse.txt


# =================================================================
# 3. 密集矩阵优化参数敏感度测试 (N=10000)
# =================================================================
echo "=== 3/5: 密集矩阵优化参数敏感度测试 ===" >> results_dense.txt

# Grid 维度敏感度 (Block=1000, N=10000)
echo "--- Dense Grid Dim Sensitivity (N=10000, B=1000) ---" >> results_dense.txt
for grid in 1 2 4 5
do
    run_experiment "$SPARK_ARGS" dense_optimized_grid.py 10000 $DEFAULT_BLOCK $grid results_dense.txt
done

# 块大小敏感度 (Grid=2x2, N=10000)
echo "--- Dense Block Size Sensitivity (N=10000, Grid=2) ---" >> results_dense.txt
for block in 500 1000 2000
do
   run_experiment "$SPARK_ARGS" dense_optimized_grid.py 10000 $block 2 results_dense.txt
done


# =================================================================
# 4. 稀疏矩阵稀疏度敏感度测试 (N=10000)
# =================================================================
echo "=== 4/5: 稀疏矩阵稀疏度敏感度测试 (Sparsity Sensitivity) ===" >> results_sparse.txt
for sparsity in 0.01 0.1 0.5 1.0
do
   echo "--- Sparse Sensitivity Test N=10000, S=$sparsity ---" >> results_sparse.txt
   run_experiment "$SPARK_ARGS" sparse_optimized_df.py 10000 $sparsity results_sparse.txt
   run_experiment "$SPARK_ARGS" sparse_optimized_csr.py 10000 $sparsity results_sparse.txt # 加入 CSR
   run_experiment "$SPARK_ARGS" systemds_runner.py 10000 $sparsity results_systemds.txt
done


# =================================================================
# 5. 矩阵扩展性测试 (Scale Test)
# =================================================================
echo "=== 5/5: 矩阵扩展性测试 (Scale Test) ===" >> results_sparse.txt

# 5.1 稀疏矩阵扩展性测试 (S=0.01) - 加入 CSR 优化
echo "--- Sparse Scale Test (S=0.01) ---" >> results_sparse.txt
for n in 10000 20000 30000 40000
do
   echo "--- Sparse Scale Test N=$n ---" >> results_sparse.txt
   run_experiment "$SPARK_ARGS" sparse_optimized_df.py $n $DEFAULT_SPARSITY results_sparse.txt
   run_experiment "$SPARK_ARGS" sparse_optimized_csr.py $n $DEFAULT_SPARSITY results_sparse.txt
   run_experiment "$SPARK_ARGS" systemds_runner.py $n $DEFAULT_SPARSITY results_systemds.txt
done

# 5.2 密集矩阵扩展性测试 (S=1.0) - 确保 Baseline 被保留
echo "--- Dense Scale Test (S=1.0) ---" >> results_dense.txt
for n in 10000 20000 30000
do
   echo "--- Dense Scale Test N=$n ---" >> results_dense.txt
   run_experiment "$SPARK_ARGS" dense_baseline.py $n $DEFAULT_BLOCK results_dense.txt # <<< 修正：保留 Baseline >>>
   run_experiment "$SPARK_ARGS" dense_optimized_grid.py $n $DEFAULT_BLOCK 2 results_dense.txt
   run_experiment "$SPARK_ARGS" systemds_runner.py $n 1.0 results_systemds.txt
done


echo "所有实验已完成。请检查 results_*.txt 文件。"
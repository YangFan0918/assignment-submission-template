#!/bin/bash

# =================================================================
# 统一的执行函数 (与原脚本保持一致)
# =================================================================
function run_experiment() {
    LOCAL_SPARK_ARGS="$1"
    SCRIPT_NAME="$2"
    N="$3"
    PARAM="$4"
    LOG_FILE="$5"
    
    # 特殊处理 Arrow 优化 (如果需要，可在这里配置)
    if [[ "$SCRIPT_NAME" == "dense_optimized_arrow.py" ]]; then
        LOCAL_SPARK_ARGS="$LOCAL_SPARK_ARGS --conf spark.sql.execution.arrow.pyspark.enabled=true"
    fi
    
    echo "Running $SCRIPT_NAME N=$N P=$PARAM with args: $LOCAL_SPARK_ARGS" >> $LOG_FILE
    spark-submit $LOCAL_SPARK_ARGS $SCRIPT_NAME $N $PARAM >> $LOG_FILE
}


# =================================================================
# 统一基准配置 (与原脚本保持一致)
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
echo -e "\n\n======== FINAL SCALE & SENSITIVITY RUN STARTED at $START_TIME ========\n" >> results_dense.txt
echo -e "\n\n======== FINAL SCALE & SENSITIVITY RUN STARTED at $START_TIME ========\n" >> results_sparse.txt
echo -e "\n\n======== FINAL SCALE & SENSITIVITY RUN STARTED at $START_TIME ========\n" >> results_systemds.txt


# =================================================================
# 4. 稀疏矩阵稀疏度敏感度测试 (N=10000) - 仅运行 SystemDS S=0.5, 1.0
#    * 优化脚本 (DF/CSR) 会 OOM，因此只跑 SystemDS 作为对比。
# =================================================================
echo "=== 4/5: 稀疏矩阵稀疏度敏感度测试 (Sparsity Sensitivity) - 仅运行 S=0.5, 1.0 SystemDS ===" >> results_sparse.txt

# 运行 S=0.5
sparsity=0.5
echo "--- Sparse Sensitivity Test N=10000, S=$sparsity ---" >> results_sparse.txt
# DF/CSR 优化脚本会 OOM，跳过。
run_experiment "$SPARK_ARGS" systemds_runner.py 10000 $sparsity results_systemds.txt 
   
# 运行 S=1.0
sparsity=1.0
echo "--- Sparse Sensitivity Test N=10000, S=$sparsity ---" >> results_sparse.txt
# DF/CSR 优化脚本会 OOM，跳过。
run_experiment "$SPARK_ARGS" systemds_runner.py 10000 $sparsity results_systemds.txt 


# =================================================================
# 5. 矩阵扩展性测试 (Scale Test) - 运行所有
# =================================================================
echo "=== 5/5: 矩阵扩展性测试 (Scale Test) ===" >> results_sparse.txt

# 5.1 稀疏矩阵扩展性测试 (S=0.01) - 运行所有
echo "--- Sparse Scale Test (S=0.01) ---" >> results_sparse.txt
for n in 10000 20000 30000 40000
do
   echo "--- Sparse Scale Test N=$n ---" >> results_sparse.txt
   run_experiment "$SPARK_ARGS" sparse_optimized_df.py $n $DEFAULT_SPARSITY results_sparse.txt
   run_experiment "$SPARK_ARGS" sparse_optimized_csr.py $n $DEFAULT_SPARSITY results_sparse.txt
   run_experiment "$SPARK_ARGS" systemds_runner.py $n $DEFAULT_SPARSITY results_systemds.txt
done

# 5.2 密集矩阵扩展性测试 (S=1.0) - 运行所有
echo "--- Dense Scale Test (S=1.0) ---" >> results_dense.txt
for n in 10000 20000 30000
do
   echo "--- Dense Scale Test N=$n ---" >> results_dense.txt
   run_experiment "$SPARK_ARGS" dense_baseline.py $n $DEFAULT_BLOCK results_dense.txt
   run_experiment "$SPARK_ARGS" dense_optimized_grid.py $n $DEFAULT_BLOCK 2 results_dense.txt
   run_experiment "$SPARK_ARGS" systemds_runner.py $n 1.0 results_systemds.txt
done


echo "所有剩余扩展性和敏感度测试已安排运行。请检查 results_*.txt 文件。"
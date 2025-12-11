# sparse_optimized_df.py (DataFrame Broadcast Optimized - Executor Parallel Data Generation)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, broadcast, udf, lit
from pyspark.sql.types import LongType, DoubleType
import random
import time
import sys
from tools_util import get_application_metrics, format_bytes, format_time_ms, log_result_to_file

# --- 实验参数 ---
N = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
SPARSITY = float(sys.argv[2]) if len(sys.argv) > 2 else 0.01
NUM_PARTITIONS = 24
LOG_FILE = "results_sparse.txt"

spark = SparkSession.builder.appName(f"SparseMul_DF_Opt_N{N}_S{SPARSITY}").getOrCreate()
sc = spark.sparkContext

# --- UDF 定义：在 Executor 端并行生成随机值和坐标 ---

# UDF for generating a random value (0 to 1)
@udf(DoubleType())
def random_value_udf():
    return random.random()

# UDF for generating a random coordinate (0 to N-1)
# Note: Use LongType for large N
@udf(LongType())
def random_coord_udf(n_max):
    # n_max 是 N (矩阵维度)
    return random.randint(0, n_max - 1)

# --- 业务逻辑 ---
def generate_sparse_df(rows, cols, sparsity, matrix_name):
    """
    生成稀疏矩阵的 DataFrame: (row_id, col_id, value)
    FIX: 使用 Executor 并行生成数据，避免 Driver 瓶颈。
    """
    total_elements = int(rows * cols * sparsity)
    
    # 1. 创建一个包含 N*N*S 个索引的 DataFrame，并分区到所有 Executor
    #    这样后续的 UDF 就能并行执行。
    index_df = spark.range(total_elements).repartition(NUM_PARTITIONS)

    # 2. 并行生成坐标和值
    if matrix_name == 'A':
        # 矩阵 A: (i, k, val)
        df = index_df.withColumn('i', random_coord_udf(lit(rows))) \
                     .withColumn('k', random_coord_udf(lit(cols))) \
                     .withColumn('val', random_value_udf())
        columns = ['i', 'k', 'val']
    else: # matrix_name == 'B'
        # 矩阵 B: (k, j, val)
        df = index_df.withColumn('k', random_coord_udf(lit(rows))) \
                     .withColumn('j', random_coord_udf(lit(cols))) \
                     .withColumn('val', random_value_udf())
        columns = ['k', 'j', 'val']
        
    return df.select(*columns)

try:
    log_result_to_file(LOG_FILE, "="*50)
    log_result_to_file(LOG_FILE, f"Type: Sparse (DataFrame Broadcast Opt - Executor Gen) | N: {N} | Sparsity: {SPARSITY}")
    start_time = time.time()

    mat_A = generate_sparse_df(N, N, SPARSITY, 'A')
    mat_B = generate_sparse_df(N, N, SPARSITY, 'B')

    # 核心优化：使用 broadcast hint 提示 Spark 将 mat_B 广播
    joined_df = mat_A.join(
        broadcast(mat_B), 
        on=mat_A.k == mat_B.k, 
        how='inner'
    ).select(
        col('i'),
        col('j'),
        (mat_A.val * mat_B.val).alias('product')
    )

    # 聚合结果
    result_df = joined_df.groupBy('i', 'j').agg(
        spark_sum('product').alias('value')
    )

    # 触发计算
    count = result_df.count() 
    
    end_time = time.time()
    
    # 强制等待，确保 Spark Metrics 稳定
    time.sleep(5) 

    s_read, s_write, run_time_ms, gc_time_ms, total_tasks, s_read_time_ms, serialize_time_ms = get_application_metrics(sc)

    log_result_to_file(LOG_FILE, f"Status: Success | Non-zero elements in C: {count}")
    log_result_to_file(LOG_FILE, f"Duration: {end_time - start_time:.4f} s (Wall Clock)")
    log_result_to_file(LOG_FILE, f"Total Tasks: {total_tasks}")
    log_result_to_file(LOG_FILE, f"--- Time Metrics (Aggregated) ---")
    log_result_to_file(LOG_FILE, f"Executor Run Time: {format_time_ms(run_time_ms)}")
    log_result_to_file(LOG_FILE, f"Total GC Time: {format_time_ms(gc_time_ms)}")
    log_result_to_file(LOG_FILE, f"Total Shuffle Read Time: {format_time_ms(s_read_time_ms)}")
    log_result_to_file(LOG_FILE, f"Total Serialization Time: {format_time_ms(serialize_time_ms)}")
    log_result_to_file(LOG_FILE, f"--- Data Metrics ---")
    log_result_to_file(LOG_FILE, f"Shuffle Read: {format_bytes(s_read)}")
    log_result_to_file(LOG_FILE, f"Shuffle Write: {format_bytes(s_write)}")
    log_result_to_file(LOG_FILE, "="*50)

except Exception as e:
    log_result_to_file(LOG_FILE, f"Error: {type(e).__name__}: {str(e)}")
    log_result_to_file(LOG_FILE, "="*50)
    raise e
finally:
    spark.stop()
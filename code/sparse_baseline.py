# sparse_baseline.py (Manual CPU Timing + Parallel Data Generation)
from pyspark.sql import SparkSession
import time
import random
import sys
from tools_util import get_application_metrics, format_bytes, format_time_ms, log_result_to_file

# --- 实验参数 ---
N = int(sys.argv[1]) if len(sys.argv) > 1 else 20000
SPARSITY = float(sys.argv[2]) if len(sys.argv) > 2 else 0.01
NUM_PARTITIONS = 24
LOG_FILE = "results_sparse.txt"

spark = SparkSession.builder.appName(f"SparseMul_Base_N{N}_S{SPARSITY}").getOrCreate()
sc = spark.sparkContext

# --- 业务逻辑 ---
def generate_sparse_matrix(rows, cols, sparsity, partitions, matrix_type='A'):
    """
    修正后的数据生成：使用 RDD flatMap 在 Executor 上并行生成稀疏三元组。
    """
    total_elements = int(rows * cols * sparsity)
    seeds = sc.parallelize(range(partitions), partitions) # 创建分区种子
    
    def local_gen(seed):
        """每个 Executor 核心负责生成其分配到的数据块"""
        random.seed(seed) # 确保随机性分散
        local_data = []
        local_count = total_elements // partitions # 估算每个分区的元素数量
        # 即使这里有少量误差，但核心目的是并行化
        
        for _ in range(local_count):
            r, c, v = random.randint(0, rows-1), random.randint(0, cols-1), random.random()
            if matrix_type == 'A':
                local_data.append((c, (r, v))) # Key=col (k)
            else:
                local_data.append((r, (c, v))) # Key=row (k)
        return local_data

    # flatMap 将并行生成的列表展平为单个 RDD
    return seeds.flatMap(local_gen).repartition(partitions)


def map_multiply_with_timing(iterator):
    """在一个分区内执行 map 乘法和计时 (核心逻辑不变)"""
    start_proc_time = time.process_time()
    
    results = []
    for item in iterator:
        k, ((i, val_A), (j, val_B)) = item
        results.append( ((i, j), val_A * val_B) ) 
        
    end_proc_time = time.process_time()
    task_compute_time = end_proc_time - start_proc_time
    
    yield (("__TIME__", "PARTITION"), task_compute_time)
    for result in results:
        yield result


try:
    log_result_to_file(LOG_FILE, "="*50)
    log_result_to_file(LOG_FILE, f"Type: Sparse (Coordinate Shuffle + CPU Time - Parallel Gen) | N: {N} | Sparsity: {SPARSITY}")
    start_time = time.time()

    matrix_A = generate_sparse_matrix(N, N, SPARSITY, NUM_PARTITIONS, 'A')
    matrix_B = generate_sparse_matrix(N, N, SPARSITY, NUM_PARTITIONS, 'B')

    joined = matrix_A.join(matrix_B)

    result = joined.mapPartitions(map_multiply_with_timing) \
        .reduceByKey(lambda a, b: a + b)

    # NEW: 分离结果和计时数据
    compute_time_rdd = result.filter(lambda x: x[0][0] == "__TIME__")
    final_result_rdd = result.filter(lambda x: x[0][0] != "__TIME__")
    total_python_compute_time_s = compute_time_rdd.map(lambda x: x[1]).sum() 
    count = final_result_rdd.count()
    
    end_time = time.time()
    
    # ... (指标采集和日志输出) ...
    time.sleep(5) 

    s_read, s_write, run_time_ms, gc_time_ms, total_tasks, s_read_time_ms, serialize_time_ms = get_application_metrics(sc)

    log_result_to_file(LOG_FILE, f"Status: Success | Non-zero elements in C: {count}")
    log_result_to_file(LOG_FILE, f"Duration: {end_time - start_time:.4f} s (Wall Clock)")
    log_result_to_file(LOG_FILE, f"Total Tasks: {total_tasks}")
    log_result_to_file(LOG_FILE, f"--- Time Metrics (Aggregated) ---")
    log_result_to_file(LOG_FILE, f"Total Python Compute Time: {format_time_ms(total_python_compute_time_s * 1000)}")
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
finally:
    spark.stop()
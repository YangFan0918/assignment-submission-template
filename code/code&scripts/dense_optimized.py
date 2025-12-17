# dense_optimized.py (Manual CPU Timing Version)
from pyspark.sql import SparkSession
import numpy as np
import time
import sys
from tools_util import get_application_metrics, format_bytes, format_time_ms, log_result_to_file

N = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
BLOCK_SIZE = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
NUM_PARTITIONS = 24
LOG_FILE = "results_dense.txt"

spark = SparkSession.builder.appName(f"DenseMul_Opt_N{N}_B{BLOCK_SIZE}").getOrCreate()
sc = spark.sparkContext

# --- 业务逻辑 ---
def generate_block_matrix(rows, cols, block_size, num_partitions):
    # ... (生成逻辑保持不变) ...
    r_blocks = (rows + block_size - 1) // block_size
    c_blocks = (cols + block_size - 1) // block_size
    seeds = []
    for i in range(r_blocks):
        for j in range(c_blocks):
            cur_rows = block_size if (i+1)*block_size <= rows else rows - i*block_size
            cur_cols = block_size if (j+1)*block_size <= cols else cols - j*block_size
            seeds.append(((i, j), (cur_rows, cur_cols)))
    return sc.parallelize(seeds, num_partitions).map(
        lambda x: (x[0], np.random.rand(x[1][0], x[1][1]))
    )

def rdd_map_multiply(item):
    (block_i, block_k), mat_A_sub = item
    full_B = B_broadcast.value
    relevant_B_keys = [key for key in full_B.keys() if key[0] == block_k]
    
    results = []
    for b_key in relevant_B_keys:
        block_j = b_key[1]
        results.append( ((block_i, block_j), np.dot(mat_A_sub, full_B[b_key])) )
    return results

def map_with_timing(iterator):
    """在一个分区内执行 map 乘法和计时"""
    start_proc_time = time.process_time()
    
    results = []
    for item in iterator:
        # 执行原有的 map 逻辑
        results.extend(rdd_map_multiply(item)) 
        
    end_proc_time = time.process_time()
    task_compute_time = end_proc_time - start_proc_time
    
    yield (("__TIME__", "PARTITION"), task_compute_time)
    for result in results:
        yield result
            
try:
    # ... (日志和生成逻辑) ...
    log_result_to_file(LOG_FILE, "="*50)
    log_result_to_file(LOG_FILE, f"Type: Dense (Broadcast Opt + CPU Time) | N: {N} | Block: {BLOCK_SIZE}")

    # ... (内存警告) ...

    start_time = time.time()

    mat_A = generate_block_matrix(N, N, BLOCK_SIZE, NUM_PARTITIONS)
    mat_B_rdd = generate_block_matrix(N, N, BLOCK_SIZE, NUM_PARTITIONS)
    
    # Broadcast 核心逻辑 
    B_local = mat_B_rdd.collect() 
    B_dict = {key: matrix for key, matrix in B_local}
    global B_broadcast
    B_broadcast = sc.broadcast(B_dict)

    # 执行 RDD 链条 (计时封装)
    result = mat_A.mapPartitions(map_with_timing).reduceByKey(lambda m1, m2: m1 + m2)
    
    # NEW: 分离结果和计时数据
    compute_time_rdd = result.filter(lambda x: x[0][0] == "__TIME__")
    final_result_rdd = result.filter(lambda x: x[0][0] != "__TIME__")
    total_python_compute_time_s = compute_time_rdd.map(lambda x: x[1]).sum() 
    count = final_result_rdd.count()

    end_time = time.time()
    
    # ... (指标采集和日志输出) ...
    time.sleep(5) 

    s_read, s_write, run_time_ms, gc_time_ms, total_tasks, s_read_time_ms, serialize_time_ms = get_application_metrics(sc)

    log_result_to_file(LOG_FILE, f"Status: Success | Final Block Count: {count}")
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
    raise e
finally:
    spark.stop()
# sparse_optimized.py (Manual CPU Timing + Parallel Data Generation)
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

spark = SparkSession.builder.appName(f"SparseMul_Opt_N{N}_S{SPARSITY}").getOrCreate()
sc = spark.sparkContext

# --- 业务逻辑 ---

def generate_rdd_A(rows, cols, sparsity, partitions):
    """
    修正后的 RDD A 生成：使用 RDD flatMap 在 Executor 上并行生成 RDD A 的三元组。
    RDD A: (i, k, val)
    """
    total_elements = int(rows * cols * sparsity)
    seeds = sc.parallelize(range(partitions), partitions)
    
    def local_gen_A(seed):
        random.seed(seed)
        local_data = []
        local_count = total_elements // partitions
        for _ in range(local_count):
            i, k, val = random.randint(0, rows-1), random.randint(0, cols-1), random.random()
            local_data.append((i, k, val))
        return local_data
    
    # flatMap 将并行生成的列表展平为单个 RDD
    return seeds.flatMap(local_gen_A).repartition(partitions)

def generate_local_sparse_dict(rows, cols, sparsity, partitions):
    """
    修正后的矩阵 B 字典生成：在 Executor 上并行生成 B 的数据，然后 Collect。
    B_dict: {k: {j: val}}
    """
    total_elements = int(rows * cols * sparsity)
    seeds = sc.parallelize(range(partitions), partitions)
    
    def local_gen_B(seed):
        random.seed(seed)
        local_data = []
        local_count = total_elements // partitions
        for _ in range(local_count):
            k, j, val = random.randint(0, rows-1), random.randint(0, cols-1), random.random()
            local_data.append((k, j, val))
        return local_data
        
    # 并行生成 RDD B 的数据
    rdd_b = seeds.flatMap(local_gen_B)
    
    # 收集到 Driver
    B_local = rdd_b.collect()
    
    # 转换为字典格式 {k: {j: val}}
    data = {}
    for k, j, val in B_local:
        if k not in data: data[k] = {}
        data[k][j] = val
    return data

def map_with_timing(iterator):
    """在一个分区内执行 map 乘法和计时 (核心逻辑不变)"""
    start_proc_time = time.process_time()
    global B_bc # 确保访问广播变量
    B_dict = B_bc.value 
    
    results = []
    for item in iterator:
        row_i, col_k, val_A = item
        if col_k in B_dict:
            for col_j, val_B in B_dict[col_k].items():
                results.append(((row_i, col_j), val_A * val_B))
        
    end_proc_time = time.process_time()
    task_compute_time = end_proc_time - start_proc_time
    
    yield (("__TIME__", "PARTITION"), task_compute_time)
    for result in results:
        yield result


try:
    log_result_to_file(LOG_FILE, "="*50)
    log_result_to_file(LOG_FILE, f"Type: Sparse (Broadcast Opt + CPU Time - Parallel Gen) | N: {N} | Sparsity: {SPARSITY}")
    start_time = time.time()

    # Broadcast 核心逻辑 (现在 B 的字典生成已并行化)
    B_local = generate_local_sparse_dict(N, N, SPARSITY, NUM_PARTITIONS) 
    global B_bc
    B_bc = sc.broadcast(B_local)

    mat_A = generate_rdd_A(N, N, SPARSITY, NUM_PARTITIONS)

    result = mat_A.mapPartitions(map_with_timing).reduceByKey(lambda a,b: a+b)
    
    # ... (指标采集和日志输出保持不变) ...
    compute_time_rdd = result.filter(lambda x: x[0][0] == "__TIME__")
    final_result_rdd = result.filter(lambda x: x[0][0] != "__TIME__")
    total_python_compute_time_s = compute_time_rdd.map(lambda x: x[1]).sum() 
    count = final_result_rdd.count()
    
    end_time = time.time()
    
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
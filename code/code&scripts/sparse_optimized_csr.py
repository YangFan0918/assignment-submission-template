# sparse_optimized_csr.py (SciPy CSR Optimization + Parallel Data Generation)
from pyspark.sql import SparkSession
import time
import random
import sys
# 引入 SciPy 库
import scipy.sparse as sp
import numpy as np
from tools_util import get_application_metrics, format_bytes, format_time_ms, log_result_to_file

# --- 实验参数 ---
N = int(sys.argv[1]) if len(sys.argv) > 1 else 20000
SPARSITY = float(sys.argv[2]) if len(sys.argv) > 2 else 0.01
NUM_PARTITIONS = 24
LOG_FILE = "results_sparse.txt"

spark = SparkSession.builder.appName(f"SparseMul_Opt_N{N}_S{SPARSITY}").getOrCreate()
sc = spark.sparkContext

# --- 业务逻辑：并行数据生成 (保持不变) ---
def generate_rdd_A(rows, cols, sparsity, partitions):
    """并行生成 RDD A 的三元组 (i, k, val)"""
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
    
    return seeds.flatMap(local_gen_A).repartition(partitions)

def generate_local_sparse_csr(rows, cols, sparsity, partitions):
    """
    FIX: 生成矩阵 B 的三元组，收集后转换为 SciPy CSR 格式。
    注意：矩阵 B 用于右乘 (B)，应为 CSC 或 CSR (取决于 Join 键)。
    这里我们广播 CSR (适用于左乘 A x B)。
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
        
    rdd_b = seeds.flatMap(local_gen_B)
    B_local = rdd_b.collect()
    
    # 将三元组数据 (k, j, val) 转换为单独的数组
    data, row_ind, col_ind = zip(*B_local) if B_local else ([], [], [])
    
    # 转换为 SciPy CSR 格式 (用于高效行切片和乘法)
    # Shape 是 N x N
    if data:
        # 使用 CSR 格式
        sparse_b_csr = sp.csr_matrix((data, (row_ind, col_ind)), shape=(rows, cols))
        return sparse_b_csr
    else:
        return sp.csr_matrix((rows, cols))


def map_with_timing(iterator):
    """
    FIX: 在分区内执行 CSR 格式的矩阵乘法和计时。
    迭代器传入的是 RDD A 的三元组 (i, k, val)。
    """
    start_proc_time = time.process_time()
    
    # 1. 访问广播的 SciPy 稀疏矩阵
    B_csr = B_bc.value 
    
    # 2. 收集当前分区 RDD A 的所有三元组
    local_A_data = list(iterator)
    
    if not local_A_data:
        yield (("__TIME__", "PARTITION"), 0.0)
        return
        
    # 3. 将 RDD A 的数据（i, k, val）转换为 CSR 格式
    # RDD A 的三元组是 (row_i, col_k, val)
    data, row_ind, col_ind = zip(*local_A_data)
    
    # 转换为 SciPy CSR 矩阵 (形状 N x N)
    # RDD A 的数据通常只覆盖 N x N 矩阵的一小部分行，但必须保证其形状为 N x N
    A_csr = sp.csr_matrix((data, (row_ind, col_ind)), shape=B_csr.shape)

    # 4. 核心计算：使用 SciPy 原生矩阵乘法 (消除 Python 循环和字典查找)
    # 结果 C_csr 是一个 N x N 的稀疏矩阵
    C_csr = A_csr.dot(B_csr)

    # 5. 提取结果 C 的非零元素 (三元组格式)
    # 这一步是为了与 reduceByKey 的键值对格式兼容
    C_coo = C_csr.tocoo()
    
    results = []
    for r, c, v in zip(C_coo.row, C_coo.col, C_coo.data):
        # 键值对格式: ((row_i, col_j), value)
        results.append(((int(r), int(c)), float(v))) 

    end_proc_time = time.process_time()
    task_compute_time = end_proc_time - start_proc_time
    
    # 6. 返回计时结果和计算结果
    yield (("__TIME__", "PARTITION"), task_compute_time)
    for result in results:
        yield result


try:
    log_result_to_file(LOG_FILE, "="*50)
    log_result_to_file(LOG_FILE, f"Type: Sparse (Broadcast Opt + SciPy CSR) | N: {N} | Sparsity: {SPARSITY}")
    start_time = time.time()

    # 1. B 矩阵生成和广播 (现在是 CSR 格式)
    # 注意：我们将 B 的形状设置为 N x N
    B_csr = generate_local_sparse_csr(N, N, SPARSITY, NUM_PARTITIONS) 
    global B_bc
    B_bc = sc.broadcast(B_csr)

    # 2. A 矩阵 RDD 生成 (三元组格式)
    mat_A = generate_rdd_A(N, N, SPARSITY, NUM_PARTITIONS)

    # 3. 核心计算 RDD 链条
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
    raise e
finally:
    spark.stop()
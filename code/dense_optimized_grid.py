# dense_optimized_grid.py (Manual CPU Timing Version - ORIGINAL STATE)
from pyspark.sql import SparkSession
import numpy as np
import time 
import sys
import math
from collections import defaultdict
from tools_util import get_application_metrics, format_bytes, format_time_ms, log_result_to_file

# --- 实验参数 ---
N = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
BLOCK_SIZE = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
GRID_DIM = int(sys.argv[3]) if len(sys.argv) > 3 else 2 
LOG_FILE = "results_dense.txt"

spark = SparkSession.builder \
    .appName(f"DenseMul_Grid_N{N}_G{GRID_DIM}") \
    .config("spark.driver.maxResultSize", "4g") \
    .getOrCreate()
sc = spark.sparkContext

# --- 业务逻辑 ---
def generate_block_matrix(rows, cols, block_size, num_partitions, matrix_name):
    """生成分块的 RDD 矩阵 (key=(r, c), value=NumPy array)"""
    r_blocks = (rows + block_size - 1) // block_size
    c_blocks = (cols + block_size - 1) // block_size
    seeds = []
    for r in range(r_blocks):
        for c in range(c_blocks):
            cur_rows = block_size if (r+1)*block_size <= rows else rows - r*block_size
            cur_cols = block_size if (c+1)*block_size <= cols else cols - c*block_size
            seeds.append(((r, c), (cur_rows, cur_cols)))
            
    # <<< 恢复到原始的数据生成模式 >>>
    return sc.parallelize(seeds, num_partitions).map(
        lambda x: (x[0], np.random.rand(x[1][0], x[1][1]))
    )

def grid_computation(iter_data):
    """
    Grid 计算函数，负责在一个分区内进行局部矩阵乘法和聚合。
    加入了 time.process_time() 计时。
    """
    # 1. 记录 Task CPU 进程时间开始 (T_Compute)
    start_proc_time = time.process_time() 
    
    # 局部数据桶: 接收所有发送到本分区的数据
    buckets = defaultdict(list)
    for record in iter_data:
        (grid_i, grid_j), payload = record
        buckets[(grid_i, grid_j)].append(payload)

    results = []
    
    # 遍历每个目标 Grid 块 (GI, GJ)
    for (GI, GJ), items in buckets.items():
        A_items = [x for x in items if x[0] == 'A']
        B_items = [x for x in items if x[0] == 'B']
        if not A_items or not B_items: continue
            
        common_ks = set(x[1] for x in A_items) & set(x[1] for x in B_items)
        C_super = None 

        # 核心计算循环：迭代所有中间维度 k
        for gk in common_ks:
            # 重建 Super-block A
            A_subs = [x for x in A_items if x[1] == gk]
            A_dict = {x[2]: x[3] for x in A_subs} 
            sorted_rows = sorted(list(set(k[0] for k in A_dict.keys())))
            sorted_cols = sorted(list(set(k[1] for k in A_dict.keys())))
            A_list_2d = [[A_dict[(r, c)] for c in sorted_cols if (r, c) in A_dict] for r in sorted_rows]
            
            # 检查 A_list_2d 是否为空
            if not A_list_2d or not A_list_2d[0]:
                A_super = np.array([[]]) 
            else:
                A_super = np.block(A_list_2d) # Vectorization

            # 重建 Super-block B
            B_subs = [x for x in items if x[0] == 'B' and x[1] == gk]
            B_dict = {x[2]: x[3] for x in B_subs}
            sorted_rows_b = sorted(list(set(k[0] for k in B_dict.keys())))
            sorted_cols_b = sorted(list(set(k[1] for k in B_dict.keys())))
            B_list_2d = [[B_dict[(r, c)] for c in sorted_cols_b if (r, c) in B_dict] for r in sorted_rows_b]
            
            # 检查 B_list_2d 是否为空
            if not B_list_2d or not B_list_2d[0]:
                B_super = np.array([[]])
            else:
                B_super = np.block(B_list_2d) # Vectorization
            
            # 核心矩阵乘法 (使用原始代码的假设)
            # 确保 A 和 B 的维度兼容 (中间维度必须匹配)
            if A_super.shape[1] == B_super.shape[0]:
                partial_product = np.dot(A_super, B_super) 
                
                # 局部聚合
                if C_super is None:
                    C_super = partial_product
                else:
                    C_super += partial_product
        
        if C_super is not None:
            # <<< 恢复到原始的最终聚合：返回 Super-block 的总和 >>>
            results.append(((GI, GJ), np.sum(C_super)))
            
    # 2. 记录 Task CPU 进程时间结束
    end_proc_time = time.process_time()
    task_compute_time = end_proc_time - start_proc_time
    
    # 3. 返回计时结果和计算结果
    yield (("__TIME__", "PARTITION"), task_compute_time)
    for result in results:
        yield result

def map_A_to_grid(item):
    """矩阵 A 映射到 Grid"""
    (block_r, block_c), mat = item
    grid_r = block_r // super_step
    grid_k = block_c // super_step 
    local_r = block_r % super_step
    local_c = block_c % super_step
    results = []
    for grid_c in range(GRID_DIM):
        results.append( ((grid_r, grid_c), ('A', grid_k, (local_r, local_c), mat)) )
    return results

def map_B_to_grid(item):
    """矩阵 B 映射到 Grid"""
    (block_r, block_c), mat = item
    grid_c = block_c // super_step
    grid_k = block_r // super_step
    local_r = block_r % super_step
    local_c = block_c % super_step
    results = []
    for grid_r in range(GRID_DIM):
         results.append( ((grid_r, grid_c), ('B', grid_k, (local_r, local_c), mat)) )
    return results

try:
    log_result_to_file(LOG_FILE, "="*50)
    log_result_to_file(LOG_FILE, f"Type: Dense (2D-Grid Vectorized + CPU Time - ORIGINAL) | N: {N} | BlockSize: {BLOCK_SIZE} | Grid: {GRID_DIM}x{GRID_DIM}")
    start_time = time.time()

    blocks_per_side = (N + BLOCK_SIZE - 1) // BLOCK_SIZE
    global super_step
    super_step = math.ceil(blocks_per_side / GRID_DIM)

    # 1. 数据生成
    mat_A = generate_block_matrix(N, N, BLOCK_SIZE, 24, 'A')
    mat_B = generate_block_matrix(N, N, BLOCK_SIZE, 24, 'B')

    # 2. Grid 映射和 Shuffle
    A_mapped = mat_A.flatMap(map_A_to_grid)
    B_mapped = mat_B.flatMap(map_B_to_grid)

    result = A_mapped.union(B_mapped) \
        .partitionBy(GRID_DIM * GRID_DIM) \
        .mapPartitions(grid_computation)
    
    # 3. 分离和聚合结果
    compute_time_rdd = result.filter(lambda x: x[0][0] == "__TIME__")
    final_result_rdd = result.filter(lambda x: x[0][0] != "__TIME__")
    
    # 聚合计时结果 (T_Compute)
    total_python_compute_time_s = compute_time_rdd.map(lambda x: x[1]).sum() 
    
    # <<< 恢复到原始聚合：计算所有 Super-blocks 的总和 (触发计算) >>>
    final_sum = final_result_rdd.map(lambda x: x[1]).sum()

    end_time = time.time()
    
    # 强制等待，确保 Spark Metrics 稳定
    time.sleep(5) 
    
    # 4. 采集 Spark Metrics
    s_read, s_write, run_time_ms, gc_time_ms, total_tasks, s_read_time_ms, serialize_time_ms = get_application_metrics(sc)

    # 5. 日志输出
    log_result_to_file(LOG_FILE, f"Status: Success | Sum: {final_sum:.2f}")
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
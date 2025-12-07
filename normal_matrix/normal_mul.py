# normal_mul.py
from pyspark.sql import SparkSession
import numpy as np
import time
import datetime
import urllib.request
import json
import os

"""
zsh 1207
命令：
spark-submit \
  --master spark://master:7077 \
  --num-executors 2 \
  --executor-cores 2 \
  --executor-memory 12G \
  --driver-memory 4G \
  --conf spark.default.parallelism=24 \
  normal_mul.py
"""



# --- 实验参数 (建议 N=4000 或 8000) ---
N = 10000        
BLOCK_SIZE = 1000     
NUM_PARTITIONS = 12   
LOG_FILE = "Normal_mul_results.txt"

spark = SparkSession.builder \
    .appName(f"MatrixMul_Normal_N{N}") \
    .getOrCreate()
sc = spark.sparkContext

def get_application_metrics(sc):
    """抓取 Spark 内部 Shuffle 指标"""
    try:
        app_id = sc.applicationId
        url = f"http://localhost:4040/api/v1/applications/{app_id}/allexecutors"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode('utf-8'))
        s_read, s_write = 0, 0
        for executor in data:
            if executor['id'] != 'driver':
                s_read += executor.get('totalShuffleRead', 0)
                s_write += executor.get('totalShuffleWrite', 0)
        return s_read, s_write
    except:
        return 0, 0

def format_bytes(size):
    power = 2**10
    n = 0
    power_labels = {0 : '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"

def log_result(msg):
    print(msg)
    with open(LOG_FILE, "a") as f:
        f.write(f"[{datetime.datetime.now()}] {msg}\n")

# --- 业务逻辑 ---
def generate_block_matrix(rows, cols, block_size, num_partitions):
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

def block_multiply(iter_data):
    for item in iter_data:
        block_k, ( (block_i, mat_A), (block_j, mat_B) ) = item
        yield ((block_i, block_j), np.dot(mat_A, mat_B))

try:
    log_result("="*50)
    log_result(f"Type: Normal (Shuffle Join) | N: {N} | Block: {BLOCK_SIZE}")
    start_time = time.time()

    mat_A = generate_block_matrix(N, N, BLOCK_SIZE, NUM_PARTITIONS)
    mat_B = generate_block_matrix(N, N, BLOCK_SIZE, NUM_PARTITIONS)

    A_keyed = mat_A.map(lambda x: (x[0][1], (x[0][0], x[1])))
    B_keyed = mat_B.map(lambda x: (x[0][0], (x[0][1], x[1])))

    # 这里的 join 会触发 Shuffle
    result_rdd = A_keyed.join(B_keyed).mapPartitions(block_multiply).reduceByKey(lambda m1, m2: m1 + m2)
    count = result_rdd.count()

    end_time = time.time()
    s_read, s_write = get_application_metrics(sc)

    log_result(f"Status: Success | Blocks: {count}")
    log_result(f"Duration: {end_time - start_time:.4f} s")
    log_result(f"Shuffle Read:  {format_bytes(s_read)}")
    log_result(f"Shuffle Write: {format_bytes(s_write)}")

except Exception as e:
    log_result(f"Error: {str(e)}")
finally:
    spark.stop()
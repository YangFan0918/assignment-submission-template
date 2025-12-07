# systemds_mul.py
from pyspark.sql import SparkSession
from systemds.context import SystemDSContext
import time
import datetime
import urllib.request
import json

"""
zsh 1207
命令：
spark-submit \
  --master spark://master:7077 \
  --num-executors 2 \
  --executor-cores 2 \
  --executor-memory 12G \
  --driver-memory 1G \
  systemds_mul.py
"""


# --- 实验参数 (必须足够大以触发分布式) ---
N = 10000 
LOG_FILE = "Systemds_mul_results.txt"

spark = SparkSession.builder.appName("SystemDS_MatrixMul").getOrCreate()
sc = spark.sparkContext
sds = SystemDSContext() # 自动获取 Spark Context

# --- 工具函数 ---
def get_application_metrics(sc):
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
    except: return 0, 0

def format_bytes(size):
    power = 2**10
    n = 0
    power_labels = {0 : '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power: size /= power; n += 1
    return f"{size:.2f} {power_labels[n]}B"

def log_result(msg):
    print(msg)
    with open(LOG_FILE, "a") as f:
        f.write(f"[{datetime.datetime.now()}] {msg}\n")

try:
    log_result("="*50)
    log_result(f"Type: SystemDS (Distributed Force) | N: {N}")
    
    # 业务逻辑
    X = sds.rand(rows=N, cols=N, min=0.0, max=1.0, pdf="uniform", sparsity=1.0)
    Y = sds.rand(rows=N, cols=N, min=0.0, max=1.0, pdf="uniform", sparsity=1.0)
    res = (X @ Y).sum()

    log_result("Starting execution...")
    start_time = time.time()
    final_value = res.compute(verbose=True) # 触发计算
    end_time = time.time()

    s_read, s_write = get_application_metrics(sc)

    log_result(f"Status: Success")
    log_result(f"Duration: {end_time - start_time:.4f} s")
    log_result(f"Shuffle Read:  {format_bytes(s_read)}")
    log_result(f"Shuffle Write: {format_bytes(s_write)}")

except Exception as e:
    log_result(f"Error: {str(e)}")
finally:
    sds.close()
    spark.stop()
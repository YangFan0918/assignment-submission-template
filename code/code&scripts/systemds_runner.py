# systemds_runner.py
from pyspark.sql import SparkSession
from systemds.context import SystemDSContext
import time
import sys
from tools_util import get_application_metrics, format_bytes, format_time_ms, log_result_to_file

# --- 实验参数 (通过 sys.argv 接收) ---
N = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
SPARSITY = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0 # 1.0 为密集
LOG_FILE = "results_systemds.txt"

spark = SparkSession.builder.appName(f"SystemDS_Runner_N{N}_S{SPARSITY}").getOrCreate()
sc = spark.sparkContext

sds = None
try:
    sds = SystemDSContext() 
except TypeError:
    sds = SystemDSContext()

try:
    type_str = "Dense" if SPARSITY == 1.0 else "Sparse"
    
    log_result_to_file(LOG_FILE, "="*50)
    log_result_to_file(LOG_FILE, f"Type: SystemDS ({type_str}) | N: {N} | Sparsity: {SPARSITY}")
    
    # 业务逻辑
    X = sds.rand(rows=N, cols=N, min=0.0, max=1.0, pdf="uniform", sparsity=SPARSITY)
    Y = sds.rand(rows=N, cols=N, min=0.0, max=1.0, pdf="uniform", sparsity=SPARSITY)
    res = (X @ Y).sum()

    start_time = time.time()
    final_value = res.compute(verbose=True) 
    end_time = time.time()

    # 强制等待，确保 Spark Metrics 稳定
    time.sleep(5) 

    s_read, s_write, run_time_ms, gc_time_ms, total_tasks, s_read_time_ms, serialize_time_ms = get_application_metrics(sc)

    log_result_to_file(LOG_FILE, f"Status: Success | Result Sum: {final_value}")
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
finally:
    try:
        if sds: sds.close()
    except Exception:
        pass
    spark.stop()
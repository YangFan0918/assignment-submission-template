# dense_optimized_arrow.py (DataFrame + Pandas UDF + Arrow Optimization)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, sum as spark_sum, rand, lit
from pyspark.sql.types import StructType, StructField, LongType, DoubleType
import pandas as pd
import numpy as np
import time
import sys
import random
from tools_util import get_application_metrics, format_bytes, format_time_ms, log_result_to_file

# --- 实验参数 ---
N = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
BLOCK_SIZE = int(sys.argv[2]) if len(sys.argv) > 2 else 1000 # 保持参数兼容性
NUM_PARTITIONS = 24
LOG_FILE = "results_dense.txt"

# 必须启用 Arrow，并在配置中设置
spark = SparkSession.builder.appName(f"DenseMul_Arrow_N{N}") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
sc = spark.sparkContext

# --- 业务逻辑：并行数据生成 (O(N^2) 密集三元组) ---

def generate_correct_rdd(rows, cols, num_partitions, matrix_name):
    """
    生成正确的 N^2 个 (i, k, val) 三元组 RDD，用于模拟密集矩阵乘法。
    注意：使用 RDD flatMap 并行生成，然后转换为 DataFrame。
    """
    total_elements = rows * cols 
    seeds = sc.parallelize(range(num_partitions), num_partitions)
    
    elements_per_task = total_elements // num_partitions
    
    def local_gen(seed):
        """在 Executor 上并行生成 O(N^2) 行数据"""
        import random 
        
        local_data = []
        start_index = seed * elements_per_task
        end_index = start_index + elements_per_task
        
        for idx in range(start_index, end_index):
            i = idx // rows
            k = idx % cols
            val = random.random()
            
            if matrix_name == 'A':
                local_data.append((i, k, val)) # (i, k, val)
            else:
                local_data.append((k, i, val)) # (k, j, val)
        return local_data

    # 1. 并行生成 RDD
    rdd = seeds.flatMap(local_gen)

    # 2. 定义 Schema 并转换为 DataFrame
    schema = StructType([
        StructField("index1", LongType(), False),
        StructField("index2", LongType(), False),
        StructField("val", DoubleType(), False)
    ])
    
    df = spark.createDataFrame(rdd, schema).repartition(num_partitions)
    
    if matrix_name == 'A':
        return df.select(col("index1").alias("i"), col("index2").alias("k"), col("val"))
    else: # matrix_name == 'B'
        return df.select(col("index1").alias("k"), col("index2").alias("j"), col("val")).alias('B')


# --- UDF 定义：实现核心矩阵乘法逻辑 ---
# 定义输出 Schema: (i, j, value)
result_schema = StructType([
    StructField("i", LongType(), False),
    StructField("j", LongType(), False),
    StructField("value", DoubleType(), False),
    StructField("T_Compute", DoubleType(), False) # 用于计时
])

# 使用 Grouped Map Pandas UDF 实现乘法和聚合
@pandas_udf(result_schema, functionType="GROUPED_MAP")
def pandas_aggregate_multiply(pdf):
    """
    接收按 (i, j) 分组的 Pandas DataFrame，执行最终的向量点积和聚合。
    此 UDF 运行在 Python Worker 上，数据通过 Arrow 传输。
    """
    start_proc_time = time.process_time()
    
    # pdf 包含一个 GroupBy 组的所有中间部分积：(k, val_A, val_B)
    
    # 核心计算：向量点积求和
    # 计算 i -> j 的部分积和: sum(val_A * val_B)
    
    # 1. 计算部分积 (在 Pandas Series 上操作)
    pdf['product'] = pdf['val_A'] * pdf['val_B']
    
    # 2. 按 (i, j) 组求和 (这应该在 GroupBy 之前完成，但 Grouped Map UDF 会传递分组键)
    # 由于 Spark 的 Grouped Map UDF 会在内部按分组键执行，
    # 我们这里假设 pdf 是已经携带了 (i, j) 键的中间结果集。
    
    # NOTE: Grouped Map UDF 在 PySpark 3.x+ 中要求输入必须包含分组键。
    # 这里的输入 pdf 应该已经隐式地按 (i, j) 分组。
    
    # 3. 聚合结果
    result_series = pdf.groupby(['i', 'j'])['product'].sum()
    
    # 4. 转换回 DataFrame
    result_df = result_series.reset_index()
    result_df.columns = ['i', 'j', 'value']
    
    end_proc_time = time.process_time()
    task_compute_time = end_proc_time - start_proc_time
    
    # 计时结果作为一列附加返回
    result_df['T_Compute'] = task_compute_time 
    
    return result_df


try:
    log_result_to_file(LOG_FILE, "="*50)
    log_result_to_file(LOG_FILE, f"Type: Dense (DataFrame + Arrow UDF Opt) | N: {N}")
    start_time = time.time()

    mat_A = generate_correct_rdd(N, N, NUM_PARTITIONS, 'A')
    mat_B = generate_correct_rdd(N, N, NUM_PARTITIONS, 'B')

    # 1. 核心 Join 步骤 (产生 O(N^3) 的中间结果)
    joined_df = mat_A.join(
        mat_B, 
        on=mat_A.k == mat_B.k, 
        how='inner'
    ).select(
        col('i'),
        col('j'),
        col('A.val').alias('val_A'),
        col('B.val').alias('val_B')
    )

    # 2. GroupBy (i, j) 并应用 Pandas UDF (Arrow 优化)
    # Arrow 优化在这里生效，消除 Python/JVM 序列化开销。
    result_df = joined_df.groupBy(['i', 'j']).apply(pandas_aggregate_multiply)

    # 3. 分离计时结果和触发 Action
    # 触发 Action: count()
    count = result_df.count()

    # 聚合总的 Python 计算时间
    total_python_compute_time_s = result_df.select(spark_sum('T_Compute')).collect()[0][0]
    
    end_time = time.time()
    
    # 强制等待，确保 Spark Metrics 稳定
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
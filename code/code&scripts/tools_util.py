# tools_util.py (Final Robust Version with GC Fallback)
import urllib.request
import json
import time
import datetime
import sys

# Spark REST API 的基础 URL
SPARK_API_BASE_URL = "http://localhost:4040/api/v1/applications"

def fetch_url_with_retry(url, retries=3, timeout=15):
    """
    尝试从指定的 URL 抓取 JSON 数据，支持多次重试。
    """
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url)
            # 设置较长的超时时间 (15秒)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            # 常见的 404/500 错误，如果不是最后一次尝试，则重试
            if attempt < retries - 1 and (e.code == 404 or e.code == 500):
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Warning: HTTP Error {e.code} for {url}. Retrying in 2s...", file=sys.stderr)
                time.sleep(2)
                continue
            raise e
        except Exception as e:
            # 处理其他网络或连接错误
            if attempt < retries - 1:
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Warning: Connection Error for {url}. Retrying in 2s...", file=sys.stderr)
                time.sleep(2)
                continue
            raise e
    return None

def get_application_metrics(sc):
    """
    抓取 Spark 内部 Shuffle/Executor 关键指标 (Task 级别汇总，GC Time 具有回退机制)。
    
    返回: s_read (字节), s_write (字节), total_run_time (ms), total_gc_time (ms), 
          total_tasks (个), total_shuffle_read_time (ms), total_serialize_time (ms)
    """
    app_id = sc.applicationId
    
    # 初始化所有指标
    s_read = 0             
    s_write = 0             
    total_run_time = 0      
    total_gc_time = 0       
    total_tasks = 0         
    total_shuffle_read_time = 0 
    total_serialize_time = 0 

    try:
        # 1. 获取所有 Stage 的列表
        stages_url = f"{SPARK_API_BASE_URL}/{app_id}/stages"
        stages_list = fetch_url_with_retry(stages_url)
        
        if stages_list is None:
             raise Exception("Failed to fetch stages list after retries.")
        
        # 2. 遍历每个 Stage，获取 Task 级别的详细指标
        for stage in stages_list:
            if stage.get('status') != 'COMPLETE':
                continue

            stage_id = stage.get('stageId')
            stage_attempt_id = stage.get('attemptId', 0)
            
            # 从 Stage 顶层 API 获取 Shuffle Write Bytes (修正 Shuffle Write 归零问题)
            s_write += stage.get('shuffleWriteBytes', 0)
            
            task_list_url = f"{SPARK_API_BASE_URL}/{app_id}/stages/{stage_id}/{stage_attempt_id}/taskList?limit=10000"
            
            try:
                tasks_list = fetch_url_with_retry(task_list_url)
                
                if tasks_list is None:
                    continue

                # 累加当前 Stage 中所有 Task 的指标
                for task in tasks_list:
                    metrics = task.get('taskMetrics', {})
                    
                    total_tasks += 1
                    total_run_time += metrics.get('executorRunTime', 0) 
                    total_gc_time += metrics.get('jvmGCTime', 0)
                    
                    shuffle_read_metrics = metrics.get('shuffleReadMetrics', {})
                    
                    s_read += shuffle_read_metrics.get('remoteBytesRead', 0) + shuffle_read_metrics.get('localBytesRead', 0)
                    total_shuffle_read_time += shuffle_read_metrics.get('fetchWaitTime', 0)
                    
                    total_serialize_time += metrics.get('resultSerializationTime', 0)
                    
            except Exception as e:
                print(f"Warning: Could not fetch Task details for Stage {stage_id}/{stage_attempt_id}. Error: {e}", file=sys.stderr)
                continue
                
        # 3. GC TIME 回退机制 (Fallback Mechanism)
        # 如果 Task 级别的 jvmGCTime 结果为 0 (被忽略或报告错误)，则尝试使用 Executor 汇总 API
        if total_gc_time == 0:
            executors_url = f"{SPARK_API_BASE_URL}/{app_id}/allexecutors"
            executors_data = fetch_url_with_retry(executors_url, retries=1) # 仅尝试一次

            if executors_data:
                gc_fallback = 0
                for executor in executors_data:
                    if executor.get('id') != 'driver':
                        # 注意：Executor API 使用 totalGCTime 字段
                        gc_fallback += executor.get('totalGCTime', 0)
                
                # 如果回退机制捕获到非零 GC 时间，则使用该值
                if gc_fallback > 0:
                    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Info: jvmGCTime was zero. Falling back to Executor API GC Time ({gc_fallback} ms).", file=sys.stderr)
                    total_gc_time = gc_fallback
                    
    except Exception as e:
        print(f"Error fetching Spark metrics: {e}", file=sys.stderr)
        return 0, 0, 0, 0, 0, 0, 0

    return s_read, s_write, total_run_time, total_gc_time, total_tasks, total_shuffle_read_time, total_serialize_time

def format_bytes(size):
    """将字节数格式化为易读的单位 (KB, MB, GB, TB)"""
    power = 2**10
    n = 0
    power_labels = {0 : '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power and n < 4:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"

def format_time_ms(ms):
    """将毫秒数格式化为易读的单位 (秒, 分钟)"""
    seconds = ms / 1000
    
    if seconds < 60:
        return f"{seconds:.2f} s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f} min"
    else:
        hours = seconds / 3600
        return f"{hours:.2f} hr"

def log_result_to_file(file_name, msg):
    """统一的日志记录函数，追加写入文件"""
    with open(file_name, "a") as f:
        f.write(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")
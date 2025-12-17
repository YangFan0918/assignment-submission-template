import numpy as np
import os
import sys
import shutil
from pyspark.sql import SparkSession
from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry

ROWS = 100
COLS = 100
HDFS_ROOT = "hdfs://master:9000"
LOCAL_TMP = "/tmp/matrix_test"
HDFS_INPUT = "/input_verify"
HDFS_OUTPUT_SDS = "/output_verify_sds"

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

def cleanup():
    if os.path.exists(LOCAL_TMP):
        shutil.rmtree(LOCAL_TMP)
    os.makedirs(LOCAL_TMP)
    os.system(f"hdfs dfs -rm -r -f {HDFS_INPUT}")
    os.system(f"hdfs dfs -rm -r -f {HDFS_OUTPUT_SDS}")
    os.system(f"hdfs dfs -mkdir -p {HDFS_INPUT}")

def run_numpy(a, b):
    print("[1/5] Running Local NumPy...")
    return np.dot(a, b)

def run_systemds():
    print("[2/5] Running SystemDS...")
    dml_content = f"""
    A = read("{HDFS_ROOT}{HDFS_INPUT}/A.csv", format="csv", header=FALSE);
    B = read("{HDFS_ROOT}{HDFS_INPUT}/B.csv", format="csv", header=FALSE);
    C = A %*% B;
    write(C, "{HDFS_ROOT}{HDFS_OUTPUT_SDS}", format="csv");
    """
    with open(f"{LOCAL_TMP}/test.dml", "w") as f:
        f.write(dml_content)

    cmd = f"export SYSDS_DISTRIBUTED=1 && systemds {LOCAL_TMP}/test.dml -exec spark > {LOCAL_TMP}/sds_log.txt 2>&1"
    ret = os.system(cmd)
    if ret != 0:
        print("SystemDS Execution Failed! Check logs.")
        os.system(f"cat {LOCAL_TMP}/sds_log.txt")
        sys.exit(1)

    os.system(f"hdfs dfs -getmerge {HDFS_OUTPUT_SDS} {LOCAL_TMP}/C_sds.csv")
    return np.loadtxt(f"{LOCAL_TMP}/C_sds.csv", delimiter=",")

def run_spark(sc):
    print("[3/5] Running PySpark...")

    def parse_matrix(rdd, rows, cols):
        def parse_line(line_with_idx):
            idx, line = line_with_idx
            vals = [float(x) for x in line.split(',')]
            return [MatrixEntry(idx, c, v) for c, v in enumerate(vals)]

        return rdd.zipWithIndex().flatMap(lambda x: parse_line((x[1], x[0])))

    rdd_A = sc.textFile(f"{HDFS_ROOT}{HDFS_INPUT}/A.csv")
    rdd_B = sc.textFile(f"{HDFS_ROOT}{HDFS_INPUT}/B.csv")

    mat_A = CoordinateMatrix(parse_matrix(rdd_A, ROWS, COLS)).toBlockMatrix()
    mat_B = CoordinateMatrix(parse_matrix(rdd_B, ROWS, COLS)).toBlockMatrix()

    mat_C = mat_A.multiply(mat_B)

    return mat_C.toLocalMatrix().toArray()

def main():
    cleanup()

    print(f"Generating random matrices ({ROWS}x{COLS})...")
    mat_A = np.random.rand(ROWS, COLS)
    mat_B = np.random.rand(ROWS, COLS)

    np.savetxt(f"{LOCAL_TMP}/A.csv", mat_A, delimiter=",")
    np.savetxt(f"{LOCAL_TMP}/B.csv", mat_B, delimiter=",")
    os.system(f"hdfs dfs -put {LOCAL_TMP}/A.csv {HDFS_INPUT}/")
    os.system(f"hdfs dfs -put {LOCAL_TMP}/B.csv {HDFS_INPUT}/")

    res_numpy = run_numpy(mat_A, mat_B)

    res_sds = run_systemds()

    spark = SparkSession.builder \
        .appName("VerifyMatrixMult") \
        .master("spark://192.168.0.1:7077") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR") 
    res_spark = run_spark(sc)
    spark.stop()

    print("\n" + "="*40)
    print("       RESULT VERIFICATION       ")
    print("="*40)

    match_sds = np.allclose(res_numpy, res_sds, rtol=1e-5)
    print(f"SystemDS vs NumPy: {'✅ MATCH' if match_sds else '❌ MISMATCH'}")
    if not match_sds:
        print(f"Diff: {np.abs(res_numpy - res_sds).max()}")

    match_spark = np.allclose(res_numpy, res_spark, rtol=1e-5)
    print(f"PySpark  vs NumPy: {'✅ MATCH' if match_spark else '❌ MISMATCH'}")
    if not match_spark:
        print(f"Diff: {np.abs(res_numpy - res_spark).max()}")

    print("="*40)

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Spark Structured Streaming Application: Grep + Count by Column 

- Stage 1: Filter lines containing a pattern (Grep)
- Stage 2: Count occurrences grouped by a specific column (Aggregate)
"""

import os
import sys
import json
import time
from collections import deque

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count as spark_count, split, instr, coalesce, lit
from pyspark.sql.streaming import StreamingQueryListener

def parse_args():
    """Parse command line arguments"""
    if len(sys.argv) < 8:
        print("Usage: grep_count_by_column_simple.py <master_url> <hdfs_input_file> <hdfs_output_dir> <hdfs_checkpoint_dir> <grep_pattern> <column_index> <tasks_per_stage>")
        print("Example: grep_count_by_column_simple.py spark://master:7077 hdfs://namenode:9000/spark/data/dataset1.csv hdfs://namenode:9000/spark/output hdfs://namenode:9000/spark/checkpoint 'Street' 3 3")
        sys.exit(1)
    
    master_url = sys.argv[1]
    hdfs_input_file = sys.argv[2]
    hdfs_output_dir = sys.argv[3]
    hdfs_checkpoint_dir = sys.argv[4]
    grep_pattern = sys.argv[5]
    column_index = int(sys.argv[6])
    tasks_per_stage = int(sys.argv[7])
    
    return master_url, hdfs_input_file, hdfs_output_dir, hdfs_checkpoint_dir, grep_pattern, column_index, tasks_per_stage

# Global metrics tracking
metrics = {
    'start_time': None,  # Will be set after SparkSession creation
    'end_time': None,
    'total_time': 0,
    'input_tuples': 0,
    'stage1_tuples': 0,
    'stage2_tuples': 0,
    'output_tuples': 0,  
    'output_throughput': 0,  
    'unique_keys': set()
}

def extract_column_value(value, col_idx):
    """Extract column value from CSV line"""
    try:
        parts = []
        current = ""
        in_quotes = False
        
        for char in value:
            if char == '"':
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                parts.append(current.strip())
                current = ""
            else:
                current += char
        parts.append(current.strip())
        
        if col_idx < len(parts):
            return parts[col_idx]
        return None
    except:
        return None

def main():
    master_url, hdfs_input_file, hdfs_output_dir, hdfs_checkpoint_dir, grep_pattern, column_index, tasks_per_stage = parse_args()
    
    print(f"=== Spark Structured Streaming: Grep + Count by Column (Simple) ===")
    print(f"Master: {master_url}")
    print(f"HDFS Input File: {hdfs_input_file}")
    print(f"HDFS Output: {hdfs_output_dir}")
    print(f"Grep Pattern: {grep_pattern}")
    print(f"Column Index: {column_index}")
    print(f"Tasks per Stage: {tasks_per_stage} (for fair comparison with RainStorm)")
    print("")
    
    # Initialize Spark Session
    # Set parallelism config - controls partitions for shuffles
    spark = SparkSession.builder \
        .appName("GrepCountByColumnSimple") \
        .master(master_url) \
        .config("spark.default.parallelism", str(tasks_per_stage)) \
        .config("spark.sql.shuffle.partitions", str(tasks_per_stage)) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Start timing after SparkSession is created (exclude startup time)
    metrics['start_time'] = time.time()
    
    # Read file directly from HDFS or shared filesystem
    print(f"Reading input file: {hdfs_input_file}...")
    input_df = spark.read \
        .format("text") \
        .load(hdfs_input_file) \
        .withColumnRenamed("value", "line") \
        .withColumnRenamed("line", "value")
    
    # Repartition input to N tasks so Stage 1 filter runs with N tasks
    input_df = input_df.repartition(tasks_per_stage)
    
    # Stage 1: Grep - Filter lines containing pattern (case-sensitive)
    # Filter now runs with N tasks (inherits partitions from input_df)
    filtered_df = input_df.filter(instr(col("value"), grep_pattern) > 0)
    
    # Stage 2: Count by Column - Use built-in functions instead of UDF
    # Split by comma and extract Nth column (0-indexed, so column_index - 1)
    split_col = split(col("value"), ",")
    key_col = split_col.getItem(column_index - 1)
    # Treat missing values as empty string
    key_col = coalesce(key_col, lit(""))
    
    # Stage 2: Count by Column - groupBy shuffles and uses spark.sql.shuffle.partitions (set to N) for output partitions
    column_counts_df = filtered_df \
        .select(key_col.alias("column_value")) \
        .groupBy("column_value") \
        .agg(spark_count("*").alias("count"))
    
    # Count for metrics (minimal counts to avoid extra jobs)
    total_input = input_df.count()
    stage1_count = filtered_df.count()
    
    metrics['input_tuples'] = total_input
    metrics['stage1_tuples'] = stage1_count
    
    # Write output to HDFS
    print(f"Writing output to: {hdfs_output_dir}...")
    column_counts_df \
        .select(col("column_value").alias("key"), col("count").cast("string").alias("value")) \
        .write \
        .format("csv") \
        .option("header", "false") \
        .mode("overwrite") \
        .save(hdfs_output_dir)
    
    # Count for metrics AFTER write
    stage2_count = column_counts_df.count()
    metrics['stage2_tuples'] = stage2_count
    metrics['output_tuples'] = stage2_count  # Output tuples = final stage count
    
    # Collect unique keys for metrics
    unique_keys = column_counts_df.select("column_value").distinct().collect()
    metrics['unique_keys'] = [row.column_value for row in unique_keys]
    metrics['unique_keys_count'] = len(metrics['unique_keys'])
    
    # Calculate final metrics - Performance = Output tuples per time
    metrics['end_time'] = time.time()
    total_time = metrics['end_time'] - metrics['start_time']
    metrics['total_time'] = total_time
    metrics['output_throughput'] = metrics['output_tuples'] / total_time if total_time > 0 else 0
    
    # Save metrics
    metrics_file = "/tmp/spark_metrics.json"
    with open(metrics_file, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"\n=== Final Metrics ===")
    print(f"Total Time: {total_time:.2f}s")
    print(f"Input Tuples: {metrics['input_tuples']}")
    print(f"Stage 1 Tuples (after filter): {metrics['stage1_tuples']}")
    print(f"Stage 2 Tuples (after aggregate): {metrics['stage2_tuples']}")
    print(f"Output Tuples: {metrics['output_tuples']}")
    print(f"Unique Keys: {metrics['unique_keys_count']}")
    print(f"Output Throughput (Performance): {metrics['output_throughput']:.2f} output tuples/sec")
    print(f"Metrics saved to: {metrics_file}")
    
    spark.stop()

if __name__ == "__main__":
    main()


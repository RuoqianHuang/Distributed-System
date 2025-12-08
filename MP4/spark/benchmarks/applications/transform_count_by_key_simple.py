#!/usr/bin/env python3
"""
Spark Structured Streaming Application: Transform + Count by Key

- Stage 1: Transform - Extract 1st column as key, 2nd column as value
- Stage 2: Count occurrences grouped by key (Aggregate)
"""

import os
import sys
import json
import time
from collections import deque

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count as spark_count, split, lit, concat, hash
from pyspark.sql.streaming import StreamingQueryListener

def parse_args():
    """Parse command line arguments"""
    if len(sys.argv) < 6:
        print("Usage: transform_count_by_key_simple.py <master_url> <hdfs_input_file> <hdfs_output_dir> <hdfs_checkpoint_dir> <tasks_per_stage>")
        print("Example: transform_count_by_key_simple.py spark://master:7077 hdfs://namenode:9000/spark/data/test1.edges hdfs://namenode:9000/spark/output hdfs://namenode:9000/spark/checkpoint 3")
        sys.exit(1)
    
    master_url = sys.argv[1]
    hdfs_input_file = sys.argv[2]
    hdfs_output_dir = sys.argv[3]
    hdfs_checkpoint_dir = sys.argv[4]
    tasks_per_stage = int(sys.argv[5])
    
    return master_url, hdfs_input_file, hdfs_output_dir, hdfs_checkpoint_dir, tasks_per_stage

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

def main():
    master_url, hdfs_input_file, hdfs_output_dir, hdfs_checkpoint_dir, tasks_per_stage = parse_args()
    
    print(f"=== Spark Structured Streaming: Transform + Count by Key (Simple) ===")
    print(f"Master: {master_url}")
    print(f"HDFS Input File: {hdfs_input_file}")
    print(f"HDFS Output: {hdfs_output_dir}")
    print(f"HDFS Checkpoint: {hdfs_checkpoint_dir}")
    print(f"Tasks per Stage: {tasks_per_stage} (for fair comparison with RainStorm)")
    print("")
    
    # Initialize Spark Session
    # Set parallelism config to match RainStorm's N tasks per stage
    spark = SparkSession.builder \
        .appName("TransformCountByKeySimple") \
        .master(master_url) \
        .config("spark.default.parallelism", str(tasks_per_stage)) \
        .config("spark.sql.shuffle.partitions", str(tasks_per_stage)) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Start timing after SparkSession is created (exclude startup time)
    metrics['start_time'] = time.time()
    
    # Read file directly from HDFS
    print(f"Reading input file: {hdfs_input_file}...")
    input_df = spark.read \
        .format("text") \
        .load(hdfs_input_file) \
        .withColumnRenamed("value", "line")
    
    # Repartition input to N tasks so Stage 1 transform runs with N tasks
    input_df = input_df.repartition(tasks_per_stage)
    
    # Stage 1: Transform - Extract 1st column as key, 2nd column as value
    # Split by whitespace and extract columns
    # Format: "col1 col2" -> key=col1, value=col2
    # Transform now runs with N tasks (inherits partitions from input_df)
    transformed_df = input_df \
        .withColumn("parts", split(col("line"), r"\s+")) \
        .withColumn("key", col("parts")[0]) \
        .withColumn("value", col("parts")[1]) \
        .filter(col("key").isNotNull() & col("value").isNotNull()) \
        .select("key", "value")
    
    # Stage 2: Count by Key (Aggregate)
    # groupBy shuffles and uses spark.sql.shuffle.partitions (set to N) for output partitions
    count_by_key_df = transformed_df \
        .groupBy("key") \
        .agg(spark_count("*").alias("count"))
    
    # Count for metrics
    total_input = input_df.count()
    stage1_count = transformed_df.count()
    
    metrics['input_tuples'] = total_input
    metrics['stage1_tuples'] = stage1_count
    
    # Write output to HDFS
    print(f"Writing output to: {hdfs_output_dir}...")
    count_by_key_df \
        .select(col("key"), col("count").cast("string").alias("value")) \
        .write \
        .format("csv") \
        .option("header", "false") \
        .mode("overwrite") \
        .save(hdfs_output_dir)
    
    # Count for metrics AFTER write
    stage2_count = count_by_key_df.count()
    metrics['stage2_tuples'] = stage2_count
    metrics['output_tuples'] = stage2_count  # Output tuples = final stage count
    
    # Collect unique keys for metrics
    unique_keys = count_by_key_df.select("key").distinct().collect()
    metrics['unique_keys'] = [row.key for row in unique_keys]
    metrics['unique_keys_count'] = len(metrics['unique_keys'])
    
    # Calculate final metrics - Performance = Output tuples per time
    metrics['end_time'] = time.time()
    total_time = metrics['end_time'] - metrics['start_time']
    metrics['total_time'] = total_time
    metrics['output_throughput'] = metrics['output_tuples'] / total_time if total_time > 0 else 0
    
    # Save metrics
    metrics_file = "/tmp/spark_metrics.json"
    # Convert set to list for JSON serialization
    metrics_for_json = metrics.copy()
    metrics_for_json['unique_keys'] = list(metrics['unique_keys'])
    with open(metrics_file, 'w') as f:
        json.dump(metrics_for_json, f, indent=2)
    
    print(f"\n=== Final Metrics ===")
    print(f"Total Time: {total_time:.2f}s")
    print(f"Input Tuples: {metrics['input_tuples']}")
    print(f"Stage 1 Tuples (after transform): {metrics['stage1_tuples']}")
    print(f"Stage 2 Tuples (after count by key): {metrics['stage2_tuples']}")
    print(f"Output Tuples: {metrics['output_tuples']}")
    print(f"Unique Keys: {metrics['unique_keys_count']}")
    print(f"Output Throughput (Performance): {metrics['output_throughput']:.2f} output tuples/sec")
    print(f"Metrics saved to: {metrics_file}")
    
    spark.stop()

if __name__ == "__main__":
    main()


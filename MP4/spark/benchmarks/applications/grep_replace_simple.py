#!/usr/bin/env python3
"""
Spark Structured Streaming Application: Grep + Replace

- Stage 1: Filter lines containing a pattern (Grep)
- Stage 2: Replace pattern with another string (Replace)
"""

import os
import sys
import json
import time
from collections import deque

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, lit, concat, hash
from pyspark.sql.streaming import StreamingQueryListener

def parse_args():
    """Parse command line arguments"""
    if len(sys.argv) < 8:
        print("Usage: grep_replace_simple.py <master_url> <hdfs_input_file> <hdfs_output_dir> <hdfs_checkpoint_dir> <grep_pattern> <replace_pattern> <tasks_per_stage>")
        print("Example: grep_replace_simple.py spark://master:7077 hdfs://namenode:9000/spark/data/dataset1.csv hdfs://namenode:9000/spark/output hdfs://namenode:9000/spark/checkpoint 'Street' 'AVE' 2")
        sys.exit(1)
    
    master_url = sys.argv[1]
    hdfs_input_file = sys.argv[2]  # Single file, not directory
    hdfs_output_dir = sys.argv[3]
    hdfs_checkpoint_dir = sys.argv[4]
    grep_pattern = sys.argv[5]
    replace_pattern = sys.argv[6]
    tasks_per_stage = int(sys.argv[7])
    
    return master_url, hdfs_input_file, hdfs_output_dir, hdfs_checkpoint_dir, grep_pattern, replace_pattern, tasks_per_stage

# Global metrics tracking
metrics = {
    'start_time': None,  # Will be set after SparkSession creation
    'end_time': None,
    'total_time': 0,
    'input_tuples': 0,
    'stage1_tuples': 0,
    'stage2_tuples': 0,
    'output_tuples': 0, 
    'output_throughput': 0 
}

throughput_window = deque()

class MetricsListener(StreamingQueryListener):
    """Collect metrics like RainStorm's FlowCounter"""
    
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")
        metrics['start_time'] = time.time()
    
    def onQueryIdle(self, event):
        pass
    
    def onQueryProgress(self, event):
        progress = event.progress
        current_time = time.time()
        input_rows = progress.numInputRows
        processed_rows = progress.numInputRows
        
        metrics['batches_processed'] = progress.batchId + 1
        metrics['input_tuples'] += input_rows
        metrics['stage1_tuples'] += input_rows
        metrics['stage2_tuples'] += processed_rows
        metrics['output_tuples'] += processed_rows
        
        # Track throughput
        throughput_window.append((current_time, processed_rows))
        while throughput_window and (current_time - throughput_window[0][0]) > 1.0:
            throughput_window.popleft()
        current_throughput = sum(t[1] for t in throughput_window)
        
        print(f"[Batch {progress.batchId}] Processed {processed_rows} tuples (throughput: {current_throughput:.1f} tuples/sec)")
    
    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")
        metrics['end_time'] = time.time()

def main():
    master_url, hdfs_input_file, hdfs_output_dir, hdfs_checkpoint_dir, grep_pattern, replace_pattern, tasks_per_stage = parse_args()
    
    print(f"=== Spark Structured Streaming: Grep + Replace (Simple) ===")
    print(f"Master: {master_url}")
    print(f"HDFS Input File: {hdfs_input_file}")
    print(f"HDFS Output: {hdfs_output_dir}")
    print(f"HDFS Checkpoint: {hdfs_checkpoint_dir}")
    print(f"Grep Pattern: {grep_pattern}")
    print(f"Replace Pattern: {replace_pattern}")
    print(f"Tasks per Stage: {tasks_per_stage}")
    print("")
    
    # Initialize Spark Session
    # Set parallelism config to match RainStorm's N tasks per stage
    spark_builder = SparkSession.builder \
        .appName("GrepReplaceSimple") \
        .master(master_url) \
        .config("spark.default.parallelism", str(tasks_per_stage)) \
        .config("spark.sql.shuffle.partitions", str(tasks_per_stage))
    
    # Only set checkpoint location if using HDFS (not needed for batch processing)
    # spark_builder.config("spark.sql.streaming.checkpointLocation", hdfs_checkpoint_dir)
    
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    # Start timing after SparkSession is created (exclude startup time)
    metrics['start_time'] = time.time()
    
    # Read file directly from HDFS
    # Supports both hdfs:// and file:// protocols
    print(f"Reading input file: {hdfs_input_file}...")
    input_df = spark.read \
        .format("text") \
        .load(hdfs_input_file)
    
    # Add key column
    input_df = input_df \
        .withColumnRenamed("value", "line") \
        .withColumn("key", concat(lit("file:"), hash(col("line")).cast("string"))) \
        .withColumnRenamed("line", "value")
    
    # Repartition input to N tasks so Stage 1 filter runs with N tasks
    input_df = input_df.repartition(tasks_per_stage)
    
    # Stage 1: Grep - Filter lines containing pattern (case-sensitive)
    # Filter now runs with N tasks (inherits partitions from input_df)
    filtered_df = input_df.filter(col("value").contains(grep_pattern))
    
    # Repartition filtered data to N tasks so Stage 2 replace runs with N tasks
    filtered_df = filtered_df.repartition(tasks_per_stage)
    
    # Stage 2: Replace - Replace pattern with replace_pattern
    # Replace now runs with N tasks (inherits partitions from filtered_df)
    processed_df = filtered_df.withColumn(
        "value",
        regexp_replace(col("value"), grep_pattern, replace_pattern)
    )
    
    # Count for metrics
    total_input = input_df.count()
    stage1_count = filtered_df.count()
    stage2_count = processed_df.count()
    
    metrics['input_tuples'] = total_input
    metrics['stage1_tuples'] = stage1_count
    metrics['stage2_tuples'] = stage2_count
    metrics['output_tuples'] = stage2_count  # Output tuples = final stage count
    
    # Write output to HDFS
    print(f"Writing output to: {hdfs_output_dir}...")
    processed_df.select("key", "value") \
        .write \
        .format("csv") \
        .option("header", "false") \
        .mode("overwrite") \
        .save(hdfs_output_dir)
    
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
    print(f"Stage 2 Tuples (after transform): {metrics['stage2_tuples']}")
    print(f"Output Tuples: {metrics['output_tuples']}")
    print(f"Output Throughput (Performance): {metrics['output_throughput']:.2f} output tuples/sec")
    print(f"Metrics saved to: {metrics_file}")
    
    spark.stop()

if __name__ == "__main__":
    main()


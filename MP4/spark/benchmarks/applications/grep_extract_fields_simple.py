#!/usr/bin/env python3
"""
Spark Structured Streaming Application: Grep + Extract Fields

- Stage 1: Filter lines containing a pattern (Grep)
- Stage 2: Extract first N fields from the line
"""

import os
import sys
import json
import time
from collections import deque

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, concat_ws, lit, concat, hash, array_size, slice, when, instr
from pyspark.sql.streaming import StreamingQueryListener

def parse_args():
    """Parse command line arguments"""
    if len(sys.argv) < 8:
        print("Usage: grep_extract_fields_simple.py <master_url> <hdfs_input_file> <hdfs_output_dir> <hdfs_checkpoint_dir> <grep_pattern> <num_fields> <tasks_per_stage>")
        print("Example: grep_extract_fields_simple.py spark://master:7077 hdfs://namenode:9000/spark/data/test2.csv hdfs://namenode:9000/spark/output hdfs://namenode:9000/spark/checkpoint 'pattern' 3 3")
        sys.exit(1)
    
    master_url = sys.argv[1]
    hdfs_input_file = sys.argv[2]
    hdfs_output_dir = sys.argv[3]
    hdfs_checkpoint_dir = sys.argv[4]
    grep_pattern = sys.argv[5]
    num_fields = int(sys.argv[6])
    tasks_per_stage = int(sys.argv[7])
    
    return master_url, hdfs_input_file, hdfs_output_dir, hdfs_checkpoint_dir, grep_pattern, num_fields, tasks_per_stage

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

def extract_fields(value, num_fields, delimiter=','):
    """Extract first N fields from a line, handling CSV format"""
    try:
        parts = []
        current = ""
        in_quotes = False
        
        for char in value:
            if char == '"':
                in_quotes = not in_quotes
            elif char == delimiter and not in_quotes:
                parts.append(current.strip())
                current = ""
                if len(parts) >= num_fields:
                    break
            else:
                current += char
        
        # Add the last part if we haven't reached num_fields
        if len(parts) < num_fields:
            parts.append(current.strip())
        
        # Return first num_fields joined by delimiter
        return delimiter.join(parts[:num_fields])
    except:
        return None

def main():
    master_url, hdfs_input_file, hdfs_output_dir, hdfs_checkpoint_dir, grep_pattern, num_fields, tasks_per_stage = parse_args()
    
    print(f"=== Spark Structured Streaming: Grep + Extract Fields (Simple) ===")
    print(f"Master: {master_url}")
    print(f"HDFS Input File: {hdfs_input_file}")
    print(f"HDFS Output: {hdfs_output_dir}")
    print(f"HDFS Checkpoint: {hdfs_checkpoint_dir}")
    print(f"Grep Pattern: {grep_pattern}")
    print(f"Number of Fields to Extract: {num_fields}")
    print(f"Tasks per Stage: {tasks_per_stage} (for fair comparison with RainStorm)")
    print("")
    
    # Initialize Spark Session
    # Set parallelism config to match RainStorm's N tasks per stage
    spark = SparkSession.builder \
        .appName("GrepExtractFieldsSimple") \
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
    
    # Add key column
    input_df = input_df \
        .withColumn("key", concat(lit("file:"), hash(col("line")).cast("string"))) \
        .withColumnRenamed("line", "value")
    
    # Repartition input to N tasks so Stage 1 filter runs with N tasks
    input_df = input_df.repartition(tasks_per_stage)
    
    # Stage 1: Grep - Filter lines containing pattern (case-sensitive)
    # Filter now runs with N tasks (inherits partitions from input_df)
    filtered_df = input_df.filter(instr(col("value"), grep_pattern) > 0)
    
    # Repartition filtered data to N tasks so Stage 2 extract runs with N tasks
    filtered_df = filtered_df.repartition(tasks_per_stage)
    
    # Stage 2: Extract first N fields
    # Handle both CSV (comma-separated) and space-separated formats
    # Try comma split first (for CSV), if not enough fields, try space split
    
    # Split by comma first (for CSV files)
    # Then check if we got enough fields, otherwise split by whitespace
    # Extract now runs with N tasks (inherits partitions from filtered_df)
    extracted_df = filtered_df \
        .withColumn("fields_comma", split(col("value"), ",")) \
        .withColumn("fields_space", split(col("value"), r"\s+")) \
        .withColumn("fields", 
            when(array_size(col("fields_comma")) >= num_fields, col("fields_comma"))
            .otherwise(col("fields_space"))
        ) \
        .withColumn("first_n", slice(col("fields"), 1, num_fields)) \
        .withColumn("extracted_value", concat_ws(",", col("first_n"))) \
        .filter(col("extracted_value").isNotNull() & (col("extracted_value") != "")) \
        .select("key", col("extracted_value").alias("value"))
    
    # Count for metrics
    total_input = input_df.count()
    stage1_count = filtered_df.count()
    
    metrics['input_tuples'] = total_input
    metrics['stage1_tuples'] = stage1_count
    
    # Write output to HDFS
    print(f"Writing output to: {hdfs_output_dir}...")
    extracted_df.select("key", "value") \
        .write \
        .format("csv") \
        .option("header", "false") \
        .mode("overwrite") \
        .save(hdfs_output_dir)
    
    # Count for metrics AFTER write
    stage2_count = extracted_df.count()
    metrics['stage2_tuples'] = stage2_count
    metrics['output_tuples'] = stage2_count  # Output tuples = final stage count
    
    # Collect unique keys for metrics
    unique_keys = extracted_df.select("key").distinct().collect()
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
    print(f"Stage 1 Tuples (after filter): {metrics['stage1_tuples']}")
    print(f"Stage 2 Tuples (after extract fields): {metrics['stage2_tuples']}")
    print(f"Output Tuples: {metrics['output_tuples']}")
    print(f"Output Throughput (Performance): {metrics['output_throughput']:.2f} output tuples/sec")
    print(f"Metrics saved to: {metrics_file}")
    
    spark.stop()

if __name__ == "__main__":
    main()


#!/bin/bash
# Run a single Spark experiment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARKS_DIR="$(dirname "$SCRIPT_DIR")"
MASTER_URL="${SPARK_MASTER:-spark://fa25-cs425-b601.cs.illinois.edu:7077}"
HDFS_NAMENODE="${HDFS_NAMENODE:-hdfs://fa25-cs425-b601.cs.illinois.edu:9000}"

# Parse arguments
if [ $# -lt 4 ]; then
    echo "Usage: $0 <dataset> <application> <run_number> <output_base_dir> [grep_pattern] [replace_pattern|column_index] [tasks_per_stage]"
    echo "Example: $0 dataset1 grep_replace_simple 1 /tmp/results 'Street' 'AVE' 3"
    echo "Example: $0 test1 transform_count_by_key_simple 1 /tmp/results '' '' 3"
    echo ""
    echo "For fair comparison with RainStorm, specify the same number of tasks per stage."
    echo "If not specified, defaults to 3 tasks per stage."
    echo ""
    echo "Note: transform_count_by_key_simple doesn't need grep_pattern or replace_pattern."
    exit 1
fi

DATASET=$1
APPLICATION=$2
RUN_NUMBER=$3
OUTPUT_BASE=$4
GREP_PATTERN=${5:-"1"}
REPLACE_PATTERN_OR_COL=${6:-"2"}
TASKS_PER_STAGE=${7:-3}  # Default to 3 tasks per stage

# Set paths - handle different file extensions
if [ -f "$BENCHMARKS_DIR/../data/${DATASET}.csv" ]; then
    INPUT_FILE="$BENCHMARKS_DIR/../data/${DATASET}.csv"
    HDFS_INPUT_FILE="${HDFS_NAMENODE}/spark/data/${DATASET}.csv"
elif [ -f "$BENCHMARKS_DIR/../data/${DATASET}.edges" ]; then
    INPUT_FILE="$BENCHMARKS_DIR/../data/${DATASET}.edges"
    HDFS_INPUT_FILE="${HDFS_NAMENODE}/spark/data/${DATASET}.edges"
else
    echo "Error: Input file not found: ${DATASET}.csv or ${DATASET}.edges in $BENCHMARKS_DIR/../data/"
    exit 1
fi
HDFS_OUTPUT_DIR="${HDFS_NAMENODE}/spark/output/${DATASET}/${APPLICATION}/run_${RUN_NUMBER}"
METRICS_DIR="$OUTPUT_BASE/results/${DATASET}/${APPLICATION}/run_${RUN_NUMBER}/metrics"
LOG_DIR="$OUTPUT_BASE/results/${DATASET}/${APPLICATION}/run_${RUN_NUMBER}/logs"

mkdir -p "$METRICS_DIR" "$LOG_DIR"

# Check if input file exists locally
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file $INPUT_FILE not found"
    exit 1
fi

echo "=========================================="
echo "Running Simple Experiment: $DATASET - $APPLICATION (Run $RUN_NUMBER)"
echo "=========================================="
echo "Input File: $INPUT_FILE"
echo "HDFS Input: $HDFS_INPUT_FILE"
echo "HDFS Output: $HDFS_OUTPUT_DIR"
echo "Master: $MASTER_URL"
echo "Tasks per Stage: $TASKS_PER_STAGE (for fair comparison with RainStorm)"
echo ""

# Check if hdfs command is available
HADOOP_HOME="${HADOOP_HOME:-/cs425/mp4/hadoop}"
HDFS_CMD=""

if command -v hdfs &> /dev/null; then
    HDFS_CMD="hdfs"
elif [ -f "$HADOOP_HOME/bin/hdfs" ]; then
    HDFS_CMD="$HADOOP_HOME/bin/hdfs"
    export HADOOP_HOME
    export PATH=$PATH:$HADOOP_HOME/bin
fi

# For distributed Spark, we use HDFS so all workers can access files
if [ -z "$HDFS_CMD" ]; then
    echo "Error: HDFS command not found."
    echo "For distributed Spark, you need HDFS (or similar shared storage) so all workers can access input files."
    echo ""
    echo "Options:"
    echo "1. Set up HDFS (see ../DISTRIBUTED_SETUP.md)"
    echo "2. Add Hadoop to PATH: export HADOOP_HOME=/cs425/mp4/hadoop && export PATH=\$PATH:\$HADOOP_HOME/bin"
    exit 1
fi

# Test if HDFS is actually working
if ! $HDFS_CMD dfs -ls / > /dev/null 2>&1; then
    echo "Error: HDFS command found but HDFS is not running or not accessible."
    echo "Please start HDFS: /cs425/mp4/hadoop/sbin/start-dfs.sh"
    exit 1
fi

# Upload to HDFS (required for distributed mode)
if ! $HDFS_CMD dfs -test -e "$HDFS_INPUT_FILE" 2>/dev/null; then
    echo "Uploading input file to HDFS..."
    $HDFS_CMD dfs -mkdir -p "${HDFS_NAMENODE}/spark/data" 2>&1 | grep -v "already exists" || true
    $HDFS_CMD dfs -put "$INPUT_FILE" "$HDFS_INPUT_FILE"
    echo "File uploaded to HDFS"
else
    echo "Input file already exists in HDFS"
fi

# Determine application script
if [ "$APPLICATION" == "grep_replace_simple" ]; then
    APP_SCRIPT="$BENCHMARKS_DIR/applications/grep_replace_simple.py"
elif [ "$APPLICATION" == "grep_count_by_column_simple" ]; then
    APP_SCRIPT="$BENCHMARKS_DIR/applications/grep_count_by_column_simple.py"
elif [ "$APPLICATION" == "transform_count_by_key_simple" ]; then
    APP_SCRIPT="$BENCHMARKS_DIR/applications/transform_count_by_key_simple.py"
elif [ "$APPLICATION" == "grep_extract_fields_simple" ]; then
    APP_SCRIPT="$BENCHMARKS_DIR/applications/grep_extract_fields_simple.py"
else
    echo "Error: Unknown application: $APPLICATION"
    echo "Available: grep_replace_simple, grep_count_by_column_simple, transform_count_by_key_simple, grep_extract_fields_simple"
    exit 1
fi

# Check if Spark is available
if ! command -v spark-submit &> /dev/null; then
    echo "Error: spark-submit not found"
    exit 1
fi

# Start Spark application
echo "Starting Spark application..."
if [ "$APPLICATION" == "transform_count_by_key_simple" ]; then
    echo "Command: spark-submit --master $MASTER_URL --conf spark.executor.memory=2g --conf spark.executor.cores=2 --conf spark.hadoop.fs.defaultFS=$HDFS_NAMENODE --conf spark.default.parallelism=$TASKS_PER_STAGE --conf spark.sql.shuffle.partitions=$TASKS_PER_STAGE $APP_SCRIPT $MASTER_URL $HDFS_INPUT_FILE $HDFS_OUTPUT_DIR $HDFS_NAMENODE/spark/checkpoint $TASKS_PER_STAGE"
elif [ "$APPLICATION" == "grep_extract_fields_simple" ]; then
    echo "Command: spark-submit --master $MASTER_URL --conf spark.executor.memory=2g --conf spark.executor.cores=2 --conf spark.hadoop.fs.defaultFS=$HDFS_NAMENODE --conf spark.default.parallelism=$TASKS_PER_STAGE --conf spark.sql.shuffle.partitions=$TASKS_PER_STAGE $APP_SCRIPT $MASTER_URL $HDFS_INPUT_FILE $HDFS_OUTPUT_DIR $HDFS_NAMENODE/spark/checkpoint $GREP_PATTERN $REPLACE_PATTERN_OR_COL $TASKS_PER_STAGE"
else
    echo "Command: spark-submit --master $MASTER_URL --conf spark.executor.memory=2g --conf spark.executor.cores=2 --conf spark.hadoop.fs.defaultFS=$HDFS_NAMENODE --conf spark.default.parallelism=$TASKS_PER_STAGE --conf spark.sql.shuffle.partitions=$TASKS_PER_STAGE $APP_SCRIPT $MASTER_URL $HDFS_INPUT_FILE $HDFS_OUTPUT_DIR $HDFS_NAMENODE/spark/checkpoint $GREP_PATTERN $REPLACE_PATTERN_OR_COL $TASKS_PER_STAGE"
fi
echo ""

EXPERIMENT_START=$(date +%s.%N)

# Build spark-submit command based on application type
if [ "$APPLICATION" == "transform_count_by_key_simple" ]; then
    # transform_count_by_key_simple doesn't need grep_pattern or replace_pattern
    spark-submit \
        --master "$MASTER_URL" \
        --conf "spark.executor.memory=2g" \
        --conf "spark.executor.cores=2" \
        --conf "spark.hadoop.fs.defaultFS=$HDFS_NAMENODE" \
        --conf "spark.default.parallelism=$TASKS_PER_STAGE" \
        --conf "spark.sql.shuffle.partitions=$TASKS_PER_STAGE" \
        "$APP_SCRIPT" \
        "$MASTER_URL" \
        "$HDFS_INPUT_FILE" \
        "$HDFS_OUTPUT_DIR" \
        "$HDFS_NAMENODE/spark/checkpoint" \
        "$TASKS_PER_STAGE" \
        > "$LOG_DIR/spark_app.log" 2>&1
elif [ "$APPLICATION" == "grep_extract_fields_simple" ]; then
    # grep_extract_fields_simple needs grep_pattern and num_fields
    spark-submit \
        --master "$MASTER_URL" \
        --conf "spark.executor.memory=2g" \
        --conf "spark.executor.cores=2" \
        --conf "spark.hadoop.fs.defaultFS=$HDFS_NAMENODE" \
        --conf "spark.default.parallelism=$TASKS_PER_STAGE" \
        --conf "spark.sql.shuffle.partitions=$TASKS_PER_STAGE" \
        "$APP_SCRIPT" \
        "$MASTER_URL" \
        "$HDFS_INPUT_FILE" \
        "$HDFS_OUTPUT_DIR" \
        "$HDFS_NAMENODE/spark/checkpoint" \
        "$GREP_PATTERN" \
        "$REPLACE_PATTERN_OR_COL" \
        "$TASKS_PER_STAGE" \
        > "$LOG_DIR/spark_app.log" 2>&1
else
    # Other applications need grep_pattern and replace_pattern/column_index
    spark-submit \
        --master "$MASTER_URL" \
        --conf "spark.executor.memory=2g" \
        --conf "spark.executor.cores=2" \
        --conf "spark.hadoop.fs.defaultFS=$HDFS_NAMENODE" \
        --conf "spark.default.parallelism=$TASKS_PER_STAGE" \
        --conf "spark.sql.shuffle.partitions=$TASKS_PER_STAGE" \
        "$APP_SCRIPT" \
        "$MASTER_URL" \
        "$HDFS_INPUT_FILE" \
        "$HDFS_OUTPUT_DIR" \
        "$HDFS_NAMENODE/spark/checkpoint" \
        "$GREP_PATTERN" \
        "$REPLACE_PATTERN_OR_COL" \
        "$TASKS_PER_STAGE" \
        > "$LOG_DIR/spark_app.log" 2>&1
fi
SPARK_EXIT_CODE=$?

EXPERIMENT_END=$(date +%s.%N)
EXPERIMENT_DURATION=$(python3 -c "print($EXPERIMENT_END - $EXPERIMENT_START)")

# Collect metrics
echo ""
echo "Collecting metrics..."

METRICS_JSON="/tmp/spark_metrics.json"
if [ -f "$METRICS_JSON" ]; then
    cp "$METRICS_JSON" "$METRICS_DIR/metrics.json"
    
    # Extract metrics
    TOTAL_TIME=$(python3 -c "import json; f=open('$METRICS_JSON'); d=json.load(f); print(d.get('total_time', $EXPERIMENT_DURATION))" 2>/dev/null || echo "$EXPERIMENT_DURATION")
    OUTPUT_THROUGHPUT=$(python3 -c "import json; f=open('$METRICS_JSON'); d=json.load(f); print(d.get('output_throughput', 0))" 2>/dev/null || echo "0")
    OUTPUT_TUPLES=$(python3 -c "import json; f=open('$METRICS_JSON'); d=json.load(f); print(d.get('output_tuples', 0))" 2>/dev/null || echo "0")
    INPUT_TUPLES=$(python3 -c "import json; f=open('$METRICS_JSON'); d=json.load(f); print(d.get('input_tuples', 0))" 2>/dev/null || echo "0")
    STAGE1_TUPLES=$(python3 -c "import json; f=open('$METRICS_JSON'); d=json.load(f); print(d.get('stage1_tuples', 0))" 2>/dev/null || echo "0")
    STAGE2_TUPLES=$(python3 -c "import json; f=open('$METRICS_JSON'); d=json.load(f); print(d.get('stage2_tuples', 0))" 2>/dev/null || echo "0")
else
    TOTAL_TIME=$EXPERIMENT_DURATION
    OUTPUT_THROUGHPUT=0
    OUTPUT_TUPLES=0
    INPUT_TUPLES=0
    STAGE1_TUPLES=0
    STAGE2_TUPLES=0
fi

# Download output from HDFS
echo "Downloading output from HDFS..."
OUTPUT_DOWNLOAD_DIR="$OUTPUT_BASE/results/${DATASET}/${APPLICATION}/run_${RUN_NUMBER}/output"
rm -rf "$OUTPUT_DOWNLOAD_DIR"
mkdir -p "$OUTPUT_DOWNLOAD_DIR"

# Download files - handle both direct files and nested directory structure
$HDFS_CMD dfs -get "$HDFS_OUTPUT_DIR"/* "$OUTPUT_DOWNLOAD_DIR/" 2>&1 | grep -v "already exists" || true

# If files were downloaded to a nested directory (e.g., run_1/), move them up
if [ -d "$OUTPUT_DOWNLOAD_DIR/run_${RUN_NUMBER}" ]; then
    mv "$OUTPUT_DOWNLOAD_DIR/run_${RUN_NUMBER}"/* "$OUTPUT_DOWNLOAD_DIR/" 2>/dev/null || true
    rmdir "$OUTPUT_DOWNLOAD_DIR/run_${RUN_NUMBER}" 2>/dev/null || true
fi

# Combine part files from different tasks into a single file
echo "Combining part files from different tasks..."
COMBINED_OUTPUT="$OUTPUT_DOWNLOAD_DIR/output.csv"
# Find the most recent run's UUID (in case there are multiple runs)
LATEST_CSV=$(ls -t "$OUTPUT_DOWNLOAD_DIR"/*.csv 2>/dev/null | head -1)
if [ -n "$LATEST_CSV" ]; then
    # Extract UUID from the most recent file
    UUID=$(basename "$LATEST_CSV" | sed 's/part-[0-9]*-\([^-]*\)-.*/\1/')
    # Combine all part files from the most recent run (sorted by part number)
    ls "$OUTPUT_DOWNLOAD_DIR"/part-*-${UUID}*.csv 2>/dev/null | sort | xargs cat > "$COMBINED_OUTPUT" 2>/dev/null
    if [ -f "$COMBINED_OUTPUT" ] && [ -s "$COMBINED_OUTPUT" ]; then
        COMBINED_LINES=$(wc -l < "$COMBINED_OUTPUT")
        echo "Combined output saved to: $COMBINED_OUTPUT"
        echo "  Total lines: $COMBINED_LINES"
        echo "  Part files combined: $(ls "$OUTPUT_DOWNLOAD_DIR"/part-*-${UUID}*.csv 2>/dev/null | wc -l)"
    else
        # Fallback: combine all CSV files if UUID extraction failed
        ls "$OUTPUT_DOWNLOAD_DIR"/*.csv 2>/dev/null | sort | xargs cat > "$COMBINED_OUTPUT" 2>/dev/null
        if [ -f "$COMBINED_OUTPUT" ] && [ -s "$COMBINED_OUTPUT" ]; then
            COMBINED_LINES=$(wc -l < "$COMBINED_OUTPUT")
            echo "Combined output saved to: $COMBINED_OUTPUT (all files)"
            echo "  Total lines: $COMBINED_LINES"
        fi
    fi
else
    echo "Warning: No CSV files found to combine"
    COMBINED_OUTPUT=""
fi

# Create summary
SUMMARY_FILE="$METRICS_DIR/summary.txt"
cat > "$SUMMARY_FILE" << EOF
Experiment Summary
=================================================
Dataset: $DATASET
Application: $APPLICATION
Run Number: $RUN_NUMBER
Input File: $INPUT_FILE
HDFS Input: $HDFS_INPUT_FILE
HDFS Output: $HDFS_OUTPUT_DIR
Grep Pattern: $GREP_PATTERN
Replace/Column: $REPLACE_PATTERN_OR_COL
Tasks per Stage: $TASKS_PER_STAGE

Results
=================================
Total Time: ${TOTAL_TIME}s
Input Tuples: $INPUT_TUPLES
Stage 1 Tuples (after filter): $STAGE1_TUPLES
Stage 2 Tuples (after transform/aggregate): $STAGE2_TUPLES
Output Tuples: $OUTPUT_TUPLES
Output Throughput (Performance): ${OUTPUT_THROUGHPUT} output tuples/sec
Experiment Duration: ${EXPERIMENT_DURATION}s

Output Files
============
Part files location: $OUTPUT_DOWNLOAD_DIR
EOF

if [ -n "$COMBINED_OUTPUT" ] && [ -f "$COMBINED_OUTPUT" ]; then
    echo "" >> "$SUMMARY_FILE"
    echo "Combined Output: $COMBINED_OUTPUT" >> "$SUMMARY_FILE"
    echo "  (All part files from different tasks combined into single file)" >> "$SUMMARY_FILE"
    echo "  Total lines: $(wc -l < "$COMBINED_OUTPUT")" >> "$SUMMARY_FILE"
fi

cat >> "$SUMMARY_FILE" << EOF

Logs
====
Spark App: $LOG_DIR/spark_app.log
EOF

cat "$SUMMARY_FILE"
echo ""
echo "Results saved to: $OUTPUT_BASE/results/${DATASET}/${APPLICATION}/run_${RUN_NUMBER}"
echo "=========================================="


#!/bin/bash
# Run all Spark experiments (simple version - reads files directly)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARKS_DIR="$(dirname "$SCRIPT_DIR")"

OUTPUT_BASE="${SPARK_BENCHMARK_OUTPUT:-/tmp/spark_benchmarks}"
NUM_RUNS="${NUM_RUNS:-3}"
TASKS_PER_STAGE="${TASKS_PER_STAGE:-3}"  # Default to 3, but should match RainStorm's Ntasks_per_stage
MASTER_URL="${SPARK_MASTER:-spark://fa25-cs425-b601.cs.illinois.edu:7077}"
HDFS_NAMENODE="${HDFS_NAMENODE:-hdfs://fa25-cs425-b601.cs.illinois.edu:9000}"

if [ $# -ge 1 ]; then
    OUTPUT_BASE=$1
fi
if [ $# -ge 2 ]; then
    NUM_RUNS=$2
fi
if [ $# -ge 3 ]; then
    TASKS_PER_STAGE=$3
fi

RUN_SCRIPT="$BENCHMARKS_DIR/scripts/run_experiment_simple.sh"
EXPERIMENTS=(
    "test1 grep_replace_simple 1 2"
    "test1 transform_count_by_key_simple '' ''"
    "test2 grep_count_by_column_simple Champaign 3"
    "test2 grep_extract_fields_simple AERIAL 3"
)

echo "=========================================="
echo "Running All Simple Spark Experiments"
echo "=========================================="
echo "Output Base: $OUTPUT_BASE"
echo "Runs per experiment: $NUM_RUNS"
echo "Tasks per Stage: $TASKS_PER_STAGE"
echo "Master: $MASTER_URL"
echo "HDFS Namenode: $HDFS_NAMENODE"
echo ""

TOTAL_EXPERIMENTS=$((${#EXPERIMENTS[@]} * NUM_RUNS))
CURRENT=0

for exp_config in "${EXPERIMENTS[@]}"; do
    read -r dataset app pattern replace_or_col <<< "$exp_config"
    
    # Handle empty strings (remove quotes if present)
    pattern=$(echo "$pattern" | sed "s/^'\(.*\)'$/\1/")
    replace_or_col=$(echo "$replace_or_col" | sed "s/^'\(.*\)'$/\1/")
    
    echo "=========================================="
    echo "Experiment: $dataset - $app"
    echo "Pattern: $pattern, Replace/Col: $replace_or_col"
    echo "=========================================="
    
    for run in $(seq 1 $NUM_RUNS); do
        CURRENT=$((CURRENT + 1))
        echo ""
        echo "[$CURRENT/$TOTAL_EXPERIMENTS] Run $run of $NUM_RUNS"
        echo "----------------------------------------"
        
        export SPARK_MASTER=$MASTER_URL
        export HDFS_NAMENODE=$HDFS_NAMENODE
        "$RUN_SCRIPT" "$dataset" "$app" "$run" "$OUTPUT_BASE" "$pattern" "$replace_or_col" "$TASKS_PER_STAGE"
        
        if [ $run -lt $NUM_RUNS ]; then
            echo "Waiting 5 seconds before next run..."
            sleep 5
        fi
    done
    
    echo ""
    echo "Completed all runs for $dataset - $app"
    echo ""
done

echo "=========================================="
echo "All Experiments Complete!"
echo "=========================================="
echo ""
echo "Results saved to: $OUTPUT_BASE/results"
echo ""
echo "Next steps:"
echo "1. Aggregate results:"
echo "   $BENCHMARKS_DIR/scripts/aggregate_results.sh $OUTPUT_BASE/results"
echo ""
echo "2. Generate plots:"
echo "   python3 $BENCHMARKS_DIR/scripts/plot_results.py $OUTPUT_BASE/results/summary"
echo ""


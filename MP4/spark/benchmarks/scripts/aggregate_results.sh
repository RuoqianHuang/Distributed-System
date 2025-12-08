#!/bin/bash
# Aggregate results from multiple experiment runs

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARKS_DIR="$(dirname "$SCRIPT_DIR")"

if [ $# -lt 1 ]; then
    echo "Usage: $0 <results_base_dir>"
    echo "Example: $0 /tmp/spark_benchmarks/results"
    exit 1
fi

RESULTS_BASE=$1
SUMMARY_DIR="$RESULTS_BASE/summary"

mkdir -p "$SUMMARY_DIR"

echo "Aggregating results from: $RESULTS_BASE"
echo ""

# Aggregate results for each dataset and application
# Only process test1 and test2 datasets
for dataset_dir in "$RESULTS_BASE"/*/; do
    if [ ! -d "$dataset_dir" ]; then
        continue
    fi
    
    dataset=$(basename "$dataset_dir")
    
    # Skip datasets that are not test1 or test2
    if [ "$dataset" != "test1" ] && [ "$dataset" != "test2" ]; then
        continue
    fi
    
    echo "Processing dataset: $dataset"
    
    for app_dir in "$dataset_dir"/*/; do
        if [ ! -d "$app_dir" ]; then
            continue
        fi
        
        app=$(basename "$app_dir")
        echo "  Processing application: $app"
        
        # Collect metrics from all runs
        python3 << EOF
import json
import os
import statistics
from pathlib import Path

results_base = "$app_dir"
runs = []

# Find all run directories
for run_dir in sorted(Path(results_base).glob("run_*")):
    metrics_file = run_dir / "metrics" / "metrics.json"
    if metrics_file.exists():
        with open(metrics_file) as f:
            metrics = json.load(f)
            runs.append(metrics)

if not runs:
    print(f"    No metrics found for {results_base}")
    exit(0)

# Aggregate metrics
aggregated = {
    "dataset": "$dataset",
    "application": "$app",
    "num_runs": len(runs),
    "metrics": {}
}

# Calculate statistics for each metric (aligned with RainStorm metrics)
metric_names = ["total_time", "output_throughput", "input_tuples", "output_tuples", 
                "stage1_tuples", "stage2_tuples", "batches_processed", "avg_throughput_from_samples"]

for metric in metric_names:
    values = [r.get(metric, 0) for r in runs if metric in r]
    if values:
        aggregated["metrics"][metric] = {
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0,
            "min": min(values),
            "max": max(values),
            "values": values
        }

# Save aggregated results
output_file = "$SUMMARY_DIR/${dataset}_${app}_aggregated.json"
with open(output_file, 'w') as f:
    json.dump(aggregated, f, indent=2)

print(f"    Aggregated {len(runs)} runs -> {output_file}")
EOF
    done
done

echo ""
echo "Aggregation complete. Results saved to: $SUMMARY_DIR"


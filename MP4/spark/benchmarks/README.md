# Spark Streaming Benchmarks for MP4

This directory contains the benchmarking infrastructure for comparing Spark Streaming with RainStorm in **distributed cluster mode** across all 10 VMs.

## Prerequisites

**Set up according to [DISTRIBUTED_SETUP.md](../DISTRIBUTED_SETUP.md)**

1. **HDFS Cluster**: HDFS must be installed and running
   - Namenode accessible at: `hdfs://fa25-cs425-b601.cs.illinois.edu:9000`
   - See `../DISTRIBUTED_SETUP.md` for setup instructions

2. **Spark Cluster**: Spark must be installed and cluster must be running
   - Master URL: `spark://fa25-cs425-b601.cs.illinois.edu:7077`
   - Check cluster status: `http://fa25-cs425-b601.cs.illinois.edu:8080`

## Quick Start

### 1. Run a Single Experiment

#### Example: Grep + Replace Application

```bash

./scripts/run_experiment_simple.sh \
    dataset1 \
    grep_replace_simple \
    1 \
    /tmp/results \
    1 \
    2 \
    3
```

**Arguments:**
- `dataset1` - Dataset name (dataset1 or dataset2)
- `grep_replace_simple` - Application name
- `1` - Run number
- `/tmp/results` - Output base directory
- `1` - Grep pattern
- `2` - Replace pattern
- `3` - Tasks per stage (optional, default: 3)

### 2. Run All Experiments

```bash
./scripts/run_all_experiments_simple.sh /tmp/spark_benchmarks 3 3
```

**Arguments:**
- `/tmp/spark_benchmarks` - Output base directory
- `3` - Number of runs per experiment
- `3` - Tasks per stage (matching RainStorm's `Ntasks_per_stage`)

This runs all 4 experiments (2 datasets × 2 applications) with 3 runs each.

### 3. View Results

After running an experiment, results are saved to:
```
{output_base}/results/{dataset}/{application}/run_{N}/
├── metrics/
│   ├── spark_metrics.json    # Detailed metrics in JSON
│   └── summary.txt           # Human-readable summary
├── logs/
│   └── spark_app.log         # Application logs
└── output/
    ├── part-00000-*.csv      # Part files from each task
    ├── part-00001-*.csv
    ├── part-00002-*.csv
    └── output.csv            # Combined output from all tasks
```

**View combined output:**
```bash
head /tmp/results/results/dataset1/grep_replace_simple/run_1/output/output.csv
```

**View metrics:**
```bash
cat /tmp/results/results/dataset1/grep_replace_simple/run_1/metrics/summary.txt
```

### 4. Aggregate Results

```bash
./scripts/aggregate_results.sh /tmp/spark_benchmarks/results
```

This aggregates metrics from all runs and creates summary files.


## HDFS Paths

- **Input**: `hdfs://namenode:9000/spark/data/{dataset}.csv`
- **Output**: `hdfs://namenode:9000/spark/output/{dataset}/{application}/run_{N}/`

The script automatically:
- Creates these directories if they don't exist
- Uploads input files to HDFS if needed
- Downloads output files after processing
- Combines part files from different tasks into a single `output.csv` file

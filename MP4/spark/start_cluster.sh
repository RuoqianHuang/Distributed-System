#!/bin/bash

# Start Spark Cluster
# Run this on the master VM after configuration

set -e

SPARK_HOME="${SPARK_HOME:-/cs425/mp4/spark/spark-3.5.0-bin-hadoop3}"

if [ ! -d "${SPARK_HOME}" ]; then
    echo "Error: Spark not found at ${SPARK_HOME}"
    echo "Please run setup_spark.sh first!"
    exit 1
fi

echo "=========================================="
echo "Starting Spark Cluster"
echo "=========================================="
echo ""

# Check if master is already running
if pgrep -f "org.apache.spark.deploy.master.Master" > /dev/null; then
    echo "Master is already running!"
else
    echo "[1/2] Starting Spark Master..."
    ${SPARK_HOME}/sbin/start-master.sh
    sleep 3
fi

# Check if workers are already running
if pgrep -f "org.apache.spark.deploy.worker.Worker" > /dev/null; then
    echo "Workers are already running!"
else
    echo "[2/2] Starting Spark Workers..."
    echo "Note: Worker connection failures are expected if other VMs are not accessible."
    echo "For single VM testing, you can run Spark in local mode instead."
    ${SPARK_HOME}/sbin/start-workers.sh 2>&1 | grep -v "No route to host" || true
    sleep 5
fi

echo ""
echo "=========================================="
echo "Cluster Status"
echo "=========================================="
echo ""
echo "Master Web UI: http://$(hostname -f):8080"
echo ""
echo "Checking cluster status..."
${SPARK_HOME}/bin/spark-submit --version
echo ""
echo "To check worker status, visit the Web UI above"
echo ""


#!/bin/bash

# Configure Spark Master Node
# Run this ONLY on the master VM (e.g., fa25-cs425-b601)

set -e

SPARK_HOME="${SPARK_HOME:-/cs425/mp4/spark/spark-3.5.0-bin-hadoop3}"
MASTER_HOST=$(hostname -f)

echo "=========================================="
echo "Configuring Spark Master"
echo "=========================================="
echo "Master Host: ${MASTER_HOST}"
echo "Spark Home: ${SPARK_HOME}"
echo ""

if [ ! -d "${SPARK_HOME}" ]; then
    echo "Error: Spark not found at ${SPARK_HOME}"
    echo "Please run setup_spark.sh first!"
    exit 1
fi

cd ${SPARK_HOME}/conf

# Configure spark-env.sh
echo "[1/3] Configuring spark-env.sh..."
if [ ! -f spark-env.sh ]; then
    cp spark-env.sh.template spark-env.sh
fi

# Add master configuration
if ! grep -q "SPARK_MASTER_HOST" spark-env.sh; then
    echo "" >> spark-env.sh
    echo "# Master Configuration" >> spark-env.sh
    echo "export SPARK_MASTER_HOST=${MASTER_HOST}" >> spark-env.sh
    echo "export SPARK_MASTER_PORT=7077" >> spark-env.sh
    echo "export SPARK_MASTER_WEBUI_PORT=8080" >> spark-env.sh
fi

# Configure slaves file
echo "[2/3] Configuring slaves file..."
cat > slaves << 'EOF'
fa25-cs425-b602.cs.illinois.edu
fa25-cs425-b603.cs.illinois.edu
fa25-cs425-b604.cs.illinois.edu
fa25-cs425-b605.cs.illinois.edu
fa25-cs425-b606.cs.illinois.edu
fa25-cs425-b607.cs.illinois.edu
fa25-cs425-b608.cs.illinois.edu
fa25-cs425-b609.cs.illinois.edu
fa25-cs425-b610.cs.illinois.edu
EOF

echo "Workers configured:"
cat slaves

# Configure spark-defaults.conf
echo "[3/3] Configuring spark-defaults.conf..."
if [ ! -f spark-defaults.conf ]; then
    cp spark-defaults.conf.template spark-defaults.conf
fi

# Add useful defaults
cat >> spark-defaults.conf << 'EOF'

# Performance tuning
spark.executor.memory 2g
spark.executor.cores 2
spark.streaming.batchDuration 1000ms
spark.streaming.receiver.maxRate 1000
spark.streaming.backpressure.enabled true
EOF

echo ""
echo "=========================================="
echo "Master Configuration Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Copy Spark to all worker VMs (or run setup_spark.sh on each)"
echo "2. Start master: ${SPARK_HOME}/sbin/start-master.sh"
echo "3. Start workers: ${SPARK_HOME}/sbin/start-workers.sh"
echo "4. Check status: http://${MASTER_HOST}:8080"
echo ""


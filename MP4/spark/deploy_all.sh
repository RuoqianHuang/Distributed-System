#!/bin/bash

# Deploy Spark to all VMs
# This script helps deploy Spark to multiple VMs via SSH
# Usage: ./deploy_all.sh [master_vm] [worker_vm1] [worker_vm2] ...

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SETUP_SCRIPT="${SCRIPT_DIR}/setup_spark.sh"

# Default VMs
MASTER="${1:-fa25-cs425-b601.cs.illinois.edu}"
WORKERS=(
    "${2:-fa25-cs425-b602.cs.illinois.edu}"
    "${3:-fa25-cs425-b603.cs.illinois.edu}"
    "${4:-fa25-cs425-b604.cs.illinois.edu}"
    "${5:-fa25-cs425-b605.cs.illinois.edu}"
    "${6:-fa25-cs425-b606.cs.illinois.edu}"
    "${7:-fa25-cs425-b607.cs.illinois.edu}"
    "${8:-fa25-cs425-b608.cs.illinois.edu}"
    "${9:-fa25-cs425-b609.cs.illinois.edu}"
    "${10:-fa25-cs425-b610.cs.illinois.edu}"
)

echo "=========================================="
echo "Spark Deployment to All VMs"
echo "=========================================="
echo "Master: ${MASTER}"
echo "Workers: ${WORKERS[@]}"
echo ""
echo "This will deploy Spark to all VMs via SSH."
echo ""
read -p "Continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 0
fi

# Function to deploy to a VM
deploy_to_vm() {
    local vm=$1
    local is_master=$2
    
    echo "----------------------------------------"
    echo "Deploying to: ${vm}"
    echo "----------------------------------------"
    
    # Copy setup script
    echo "Copying setup script..."
    scp ${SETUP_SCRIPT} ${vm}:/tmp/setup_spark.sh || {
        echo "Error: Failed to copy setup script to ${vm}"
        return 1
    }
    
    # Run setup script
    echo "Running setup script..."
    ssh ${vm} "chmod +x /tmp/setup_spark.sh && /tmp/setup_spark.sh" || {
        echo "Error: Failed to run setup script on ${vm}"
        return 1
    }
    
    # Copy configuration if master
    if [ "$is_master" = "true" ]; then
        echo "Configuring as master..."
        scp ${SCRIPT_DIR}/configure_master.sh ${vm}:/tmp/configure_master.sh || {
            echo "Warning: Failed to copy configure script to ${vm}"
            return 1
        }
        ssh ${vm} "chmod +x /tmp/configure_master.sh && /tmp/configure_master.sh" || {
            echo "Warning: Failed to configure master on ${vm}"
            return 1
        }
    fi
    
    echo "✓ Completed: ${vm}"
    echo ""
}

# Check if setup script exists
if [ ! -f "${SETUP_SCRIPT}" ]; then
    echo "Error: Setup script not found at ${SETUP_SCRIPT}"
    exit 1
fi

# Deploy to master
echo "Deploying to MASTER..."
deploy_to_vm "${MASTER}" "true"

# Deploy to workers
echo "Deploying to WORKERS..."
for worker in "${WORKERS[@]}"; do
    if [ -n "$worker" ] && [ "$worker" != "null" ]; then
        deploy_to_vm "${worker}" "false"
    fi
done

echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. SSH to master: ssh ${MASTER}"
echo "2. Run: cd /home/ruoqian5/cs425-mp-gb6/MP4/spark && ./start_cluster.sh"
echo "3. Check status: http://${MASTER}:8080"
echo ""


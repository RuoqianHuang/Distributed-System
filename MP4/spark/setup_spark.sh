#!/bin/bash

# Spark Streaming Setup Script for CS425 MP4
# This script sets up Spark on a single VM
# Run this on each VM (master and workers)

set -e

SPARK_VERSION="3.5.0"
HADOOP_VERSION="3"
SPARK_DIR="/cs425/mp4/spark"
SPARK_HOME="${SPARK_DIR}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
SPARK_TARBALL="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TARBALL}"

echo "=========================================="
echo "Spark Streaming Setup Script"
echo "=========================================="
echo "Spark Version: ${SPARK_VERSION}"
echo "Install Directory: ${SPARK_DIR}"
echo ""

# Check if running as root
if [ "$EUID" -eq 0 ]; then 
   echo "Please do not run as root. Use sudo only when necessary."
   exit 1
fi

# Create directory
echo "[1/6] Creating directories..."
sudo mkdir -p ${SPARK_DIR}
# Make it group-writable so teammates can use it
sudo chown $USER:$USER ${SPARK_DIR}
sudo chmod 775 ${SPARK_DIR}  # Allow group read/write
cd ${SPARK_DIR}

# Check if already installed
if [ -d "${SPARK_HOME}" ]; then
    echo "Spark already installed at ${SPARK_HOME}"
    read -p "Do you want to reinstall? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Skipping installation. Using existing installation."
        exit 0
    fi
    echo "Removing existing installation..."
    rm -rf ${SPARK_HOME}
fi

# Download Spark
echo "[2/6] Downloading Spark ${SPARK_VERSION}..."
if [ ! -f "${SPARK_TARBALL}" ]; then
    wget ${SPARK_URL} || {
        echo "Failed to download Spark. Please check your internet connection."
        exit 1
    }
else
    echo "Tarball already exists, skipping download."
fi

# Extract
echo "[3/6] Extracting Spark..."
tar -xzf ${SPARK_TARBALL}
if [ ! -d "${SPARK_HOME}" ]; then
    echo "Error: Extraction failed!"
    exit 1
fi

# Make Spark installation readable by all users
chmod -R a+rX ${SPARK_HOME}

# Find Java
echo "[4/6] Configuring Java..."
JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
if [ -z "$JAVA_HOME" ] || [ ! -d "$JAVA_HOME" ]; then
    # Try alternative method
    JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
fi

if [ -z "$JAVA_HOME" ] || [ ! -d "$JAVA_HOME" ]; then
    echo "Warning: Could not find JAVA_HOME automatically."
    echo "Please set JAVA_HOME manually in spark-env.sh"
    JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
fi

echo "Using JAVA_HOME: ${JAVA_HOME}"

# Configure Spark
echo "[5/6] Configuring Spark..."
cd ${SPARK_HOME}/conf

# Create spark-env.sh
if [ ! -f spark-env.sh ]; then
    cp spark-env.sh.template spark-env.sh
fi

# Add Java configuration
if ! grep -q "JAVA_HOME" spark-env.sh; then
    echo "" >> spark-env.sh
    echo "# Java Configuration" >> spark-env.sh
    echo "export JAVA_HOME=${JAVA_HOME}" >> spark-env.sh
fi

# Add memory configuration if not present
if ! grep -q "SPARK_WORKER_MEMORY" spark-env.sh; then
    echo "" >> spark-env.sh
    echo "# Memory Configuration" >> spark-env.sh
    echo "export SPARK_WORKER_MEMORY=2g" >> spark-env.sh
    echo "export SPARK_WORKER_CORES=2" >> spark-env.sh
fi

# Create spark-defaults.conf if it doesn't exist
if [ ! -f spark-defaults.conf ]; then
    cp spark-defaults.conf.template spark-defaults.conf
fi

# Set up environment variables
echo "[6/6] Setting up environment variables..."
BASHRC="${HOME}/.bashrc"

if ! grep -q "SPARK_HOME" ${BASHRC}; then
    echo "" >> ${BASHRC}
    echo "# Spark Configuration" >> ${BASHRC}
    echo "export SPARK_HOME=${SPARK_HOME}" >> ${BASHRC}
    echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> ${BASHRC}
    echo "export JAVA_HOME=${JAVA_HOME}" >> ${BASHRC}
    echo "" >> ${BASHRC}
    echo "Environment variables added to ${BASHRC}"
    echo "Run 'source ~/.bashrc' or log out and back in to apply changes."
fi

# Verify installation
echo ""
echo "=========================================="
echo "Installation Complete!"
echo "=========================================="
echo "Spark Home: ${SPARK_HOME}"
echo "Java Home: ${JAVA_HOME}"
echo ""
echo "Next steps:"
echo "1. Run 'source ~/.bashrc' to load environment variables"
echo "2. Verify installation: ${SPARK_HOME}/bin/spark-submit --version"
echo "3. Configure master/workers (see SPARK_STREAMING_SETUP.md)"
echo ""


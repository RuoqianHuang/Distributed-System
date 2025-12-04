#!/bin/bash

# Set up environment variables for Spark
# Run this on each VM for each user

set -e

SPARK_HOME="${SPARK_HOME:-/cs425/mp4/spark/spark-3.5.0-bin-hadoop3}"

echo "=========================================="
echo "Setting up Spark Environment Variables"
echo "=========================================="
echo ""

# Check if Spark is installed
if [ ! -d "${SPARK_HOME}" ]; then
    echo "Error: Spark not found at ${SPARK_HOME}"
    echo "Please run setup_spark.sh first."
    exit 1
fi

echo "Spark found at: ${SPARK_HOME}"
echo ""

# Find Java
JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
if [ -z "$JAVA_HOME" ] || [ ! -d "$JAVA_HOME" ]; then
    JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
fi

if [ -z "$JAVA_HOME" ] || [ ! -d "$JAVA_HOME" ]; then
    echo "Warning: Could not find JAVA_HOME automatically."
    JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
fi

echo "Using JAVA_HOME: ${JAVA_HOME}"
echo ""

# Set up environment variables
BASHRC="${HOME}/.bashrc"

if ! grep -q "SPARK_HOME" ${BASHRC}; then
    echo "Adding environment variables to ${BASHRC}..."
    echo "" >> ${BASHRC}
    echo "# Spark Configuration" >> ${BASHRC}
    echo "export SPARK_HOME=${SPARK_HOME}" >> ${BASHRC}
    echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> ${BASHRC}
    echo "export JAVA_HOME=${JAVA_HOME}" >> ${BASHRC}
    echo "" >> ${BASHRC}
    echo "✓ Environment variables added!"
else
    echo "✓ Environment variables already set in ${BASHRC}"
fi

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Run 'source ~/.bashrc' or log out and back in to apply changes."
echo "Then verify with: \$SPARK_HOME/bin/spark-submit --version"
echo ""


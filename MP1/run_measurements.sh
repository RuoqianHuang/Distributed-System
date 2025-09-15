#!/bin/bash

echo "Building performance measurement tool..."
go build -o bin/performance ./cmd/performance

if [ $? -ne 0 ]; then
    echo "Failed to build performance measurement tool"
    exit 1
fi

echo "Performance measurement tool built successfully"
echo ""

echo "Starting performance measurements..."
echo "Testing with 4 VMs (vm1.log to vm4.log, 60MB each)"
echo ""

# Run the performance measurements
./bin/performance

echo ""
echo "Performance measurements complete!"

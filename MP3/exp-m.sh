#!/bin/bash

# --- Configuration ---

REPETITIONS=5
CONCURRENT_APPEND=(1 2 5 10)

# Setup and Teardown commands
UNINSTALL_CMD="ansible-playbook -i inventory.ini uninstall-playbook.yml"
INSTALL_CMD="ansible-playbook -i inventory.ini install-playbook.yml"
CLEANUP_CMD="./cleanup.sh"

# Experiment command
PLOT_CMD="./bin/plotm"

# --- Check for Binaries ---
if [ ! -f "$PLOT_CMD" ]; then
    echo "Error: Plotting command '$PLOT_CMD' not found."
    echo "Please build your binaries first (e.g., go build -o ./bin/plotm ./cmd/plotm)."
    exit 1
fi

if [ ! -f "$CLEANUP_CMD" ]; then
    echo "Error: Cleanup script '$CLEANUP_CMD' not found."
    exit 1
fi



# --- 1. Uninstall ---
echo "[RUN $i] Uninstalling cluster..."
$UNINSTALL_CMD
if [ $? -ne 0 ]; then
    echo "Error: Uninstall failed. Aborting."
    exit 1
fi

# --- 2. Cleanup ---
echo "[RUN $i] Cleaning up nodes..."
$CLEANUP_CMD
if [ $? -ne 0 ]; then
    echo "Error: Cleanup failed. Aborting."
    exit 1
fi

# --- 3. Install ---
echo "[RUN $i] Installing cluster..."
$INSTALL_CMD
if [ $? -ne 0 ]; then
    echo "Error: Install failed. Aborting."
    exit 1
fi


# --- Main Experiment Loops ---

echo "Starting HyDFS plotting script..."

# Experience iii, no delay
for nAppend in "${CONCURRENT_APPEND[@]}"; do
    echo ""
    echo "==================================================="
    echo "Starting experiments for $nAppend concurrent append"
    echo "==================================================="
    # 4KiB 
    OUTPUT_FILE="results_${nAppend}appends_4KiB_merge_no_delay.txt"
    echo "Executing: $PLOT_CMD $nAppend 5 4 0 > $OUTPUT_FILE"
    $PLOT_CMD $nAppend 20 4 0 > "$OUTPUT_FILE"
    echo "Run complete. Results saved to $OUTPUT_FILE."


    # 32KiB
    OUTPUT_FILE="results_${nAppend}appends_32KiB_merge_no_delay.txt"
    echo "Executing: $PLOT_CMD $nAppend 5 32 0 > $OUTPUT_FILE"
    $PLOT_CMD $nAppend 20 32 0 > "$OUTPUT_FILE"
    echo "Run complete. Results saved to $OUTPUT_FILE."
done

# Experience iv, 1s delay
for nAppend in "${CONCURRENT_APPEND[@]}"; do
    echo ""
    echo "==================================================="
    echo "Starting experiments for $nAppend concurrent append"
    echo "==================================================="
    # 4KiB 
    OUTPUT_FILE="results_${nAppend}appends_4KiB_merge_1s_delay.txt"
    echo "Executing: $PLOT_CMD $nAppend 5 4 1 > $OUTPUT_FILE"
    $PLOT_CMD $nAppend 20 4 1 > "$OUTPUT_FILE"
    echo "Run complete. Results saved to $OUTPUT_FILE."


    # 32KiB
    OUTPUT_FILE="results_${nAppend}appends_32KiB_merge_1s_delay.txt"
    echo "Executing: $PLOT_CMD $nAppend 5 32 1 > $OUTPUT_FILE"
    $PLOT_CMD $nAppend 20 32 1 > "$OUTPUT_FILE"
    echo "Run complete. Results saved to $OUTPUT_FILE."
done

echo "================================================="
echo "All experiments finished."
echo "================================================="
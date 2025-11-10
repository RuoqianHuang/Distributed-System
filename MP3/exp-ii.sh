#!/bin/bash

# --- Configuration ---

REPETITIONS=5
PRELOAD_FILES=(10 50 100 200 500 1000)

# Setup and Teardown commands
UNINSTALL_CMD="ansible-playbook -i inventory.ini uninstall-playbook.yml"
INSTALL_CMD="ansible-playbook -i inventory.ini install-playbook.yml"
CLEANUP_CMD="./cleanup.sh"

# Experiment command
PLOT_CMD="./bin/plotb"

# --- Check for Binaries ---
if [ ! -f "$PLOT_CMD" ]; then
    echo "Error: Plotting command '$PLOT_CMD' not found."
    echo "Please build your binaries first (e.g., go build -o ./bin/plotb ./cmd/plotb)."
    exit 1
fi

if [ ! -f "$CLEANUP_CMD" ]; then
    echo "Error: Cleanup script '$CLEANUP_CMD' not found."
    exit 1
fi

# --- Main Experiment Loop ---

echo "Starting HyDFS plotting script..."

# Outer loop: Iterate through each file size
for nFiles in "${PRELOAD_FILES[@]}"; do
    
    echo ""
    echo "================================================="
    echo "Starting experiments for $nFiles 128 KiB files"
    echo "================================================="
    
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


    # Inner loop: Run the experiment 5 times
    for (( i=1; i<=$REPETITIONS; i++ )); do
        
        echo ""
        echo "--- Run $i / $REPETITIONS (Size: $kib KiB) ---"
        
        
        # --- 4. Determine file load (Preloading) ---
        num_files_to_load=0
        if [ $i -eq 1 ]; then
            echo "[RUN $i] This is the first run. Preloading $nFiles files."
            num_files_to_load=$nFiles
        else
            echo "[RUN $i] Using preloaded files. Setting num_files to 0."
        fi
        
        # --- 5. Run Experiment & Redirect Output ---
        
        # Define a unique output file for this run
        OUTPUT_FILE="results_${nFiles}files_run_${i}.txt"
        
        echo "[RUN $i] Executing: $PLOT_CMD $num_files_to_load > $OUTPUT_FILE"
        
        # ⭐️ Added > "$OUTPUT_FILE" to redirect stdout
        $PLOT_CMD $num_files_to_load $file_size_bytes > "$OUTPUT_FILE"
        
        echo "[RUN $i] Run complete. Results saved to $OUTPUT_FILE."
        echo "-----------------------------------------"
        
    done
    
    echo "All runs for $nFiles files complete."
done

echo "================================================="
echo "All experiments finished."
echo "================================================="
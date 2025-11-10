#!/bin/bash

# --- Configuration ---

# 64KiB 128KiB, 256KiB, 512KiB, 1MiB, 2MiB, 4MiB
SIZES_KIB=(64 128 256 512 1024 2048 4096)
REPETITIONS=5
PRELOAD_FILES=100

# Setup and Teardown commands
UNINSTALL_CMD="ansible-playbook -i inventory.ini uninstall-playbook.yml"
INSTALL_CMD="ansible-playbook -i inventory.ini install-playbook.yml"
CLEANUP_CMD="./cleanup.sh"

# Experiment command
PLOT_CMD="./bin/plota"

# --- Check for Binaries ---
if [ ! -f "$PLOT_CMD" ]; then
    echo "Error: Plotting command '$PLOT_CMD' not found."
    echo "Please build your binaries first (e.g., go build -o ./bin/plota ./cmd/plota)."
    exit 1
fi

if [ ! -f "$CLEANUP_CMD" ]; then
    echo "Error: Cleanup script '$CLEANUP_CMD' not found."
    exit 1
fi

# --- Main Experiment Loop ---

echo "Starting HyDFS plotting script..."

# Outer loop: Iterate through each file size
for kib in "${SIZES_KIB[@]}"; do
    # Convert KiB to bytes
    let "file_size_bytes = kib * 1024"
    
    echo ""
    echo "================================================="
    echo "Starting experiments for File Size: $kib KiB ($file_size_bytes bytes)"
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
            echo "[RUN $i] This is the first run. Preloading $PRELOAD_FILES files."
            num_files_to_load=$PRELOAD_FILES
        else
            echo "[RUN $i] Using preloaded files. Setting num_files to 0."
        fi
        
        # --- 5. Run Experiment & Redirect Output ---
        
        # Define a unique output file for this run
        OUTPUT_FILE="results_${kib}KiB_run_${i}.txt"
        
        echo "[RUN $i] Executing: $PLOT_CMD $num_files_to_load $file_size_bytes > $OUTPUT_FILE"
        
        # ⭐️ Added > "$OUTPUT_FILE" to redirect stdout
        $PLOT_CMD $num_files_to_load $file_size_bytes > "$OUTPUT_FILE"
        
        echo "[RUN $i] Run complete. Results saved to $OUTPUT_FILE."
        echo "-----------------------------------------"

        # Restart failing node
        ssh -T -o ConnectTimeout=5 "fa25-cs425-b605.cs.illinois.edu" "sudo systemctl start MP3_server.service"
    done
    
    echo "All runs for $kib KiB complete."
done

echo "================================================="
echo "All experiments finished."
echo "================================================="
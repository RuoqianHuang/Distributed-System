#!/bin/bash

# Define the path to your client executable
CLIENT_BIN="./bin/client"

# --- Pre-run Checks ---

# 1. Check if the client binary exists
if [ ! -f "$CLIENT_BIN" ]; then
    echo "Error: Client binary not found at $CLIENT_BIN"
    echo "Please build the client first."
    exit 1
fi

# 2. Check if any arguments (file paths) were provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <local_file_path_1> <local_file_path_2> ..."
    echo "Example: $0 /path/to/file1.txt /path/to/data/file2.log"
    exit 1
fi

# --- List all member ---
"$CLIENT_BIN" list_mem_ids


# --- Main Upload Loop ---

echo "Starting batch file upload..."

# Loop through every argument provided to the script
# "$@" ensures that file paths with spaces are handled correctly
for HYDFS_FILENAME in "$@"; do
    # download file
    "$CLIENT_BIN" get "$HYDFS_FILENAME" "$HYDFS_FILENAME"

    # print replicas
    "$CLIENT_BIN" ls "$HYDFS_FILENAME"
done

echo "---"
echo "All files processed."
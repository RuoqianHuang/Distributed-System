#!/bin/bash

# --- Configuration ---
CLIENT_BIN="./bin/client"
DOWNLOAD_DEST="append.result"

# --- 1. Check Arguments ---
# We need at least 2 args: <HyDFS_Filename> and <local_file_1>
if [ $# -lt 2 ]; then
    echo "Usage: $0 <HyDFS_Filename> <local_file_1> [local_file_2] ..."
    echo "Example: $0 my_file.txt part_A.txt part_B.txt"
    exit 1
fi

# --- 2. Check Binary ---
if [ ! -f "$CLIENT_BIN" ]; then
    echo "Error: Client binary not found at $CLIENT_BIN"
    echo "Please build the client first."
    exit 1
fi

# --- 3. Separate Arguments ---
# The first argument is the HyDFS destination
HYDFS_FILENAME="$1"

# 'shift' removes the first argument ($1).
# All remaining arguments ($@) are now the local file paths.
shift

echo "--- Starting Batch Append ---"
echo "Target HyDFS File: $HYDFS_FILENAME"
echo "------------------------------"

# --- 4. Loop and Append ---
# Loop through all remaining arguments (the local files)
for LOCAL_PATH in "$@"; do
    
    # Check if the local file actually exists
    if [ ! -f "$LOCAL_PATH" ]; then
        echo "Warning: Local file not found, skipping: $LOCAL_PATH"
        continue # Skip to the next file
    fi
    
    echo "Appending '$LOCAL_PATH'..."
    
    # Run the append command
    "$CLIENT_BIN" append "$HYDFS_FILENAME" "$LOCAL_PATH"
    
    # Check the exit code of the last command
    if [ $? -ne 0 ]; then
        echo "❌ Error: Failed to append '$LOCAL_PATH'. Aborting script."
        exit 1
    fi
    
    echo "Append successful."
done

# --- 5. Download Final Result ---
echo "------------------------------"
echo "All appends complete."
echo "Downloading final file to '$DOWNLOAD_DEST'..."

"$CLIENT_BIN" get "$HYDFS_FILENAME" "$DOWNLOAD_DEST"

if [ $? -eq 0 ]; then
    echo "✅ Success: Final file downloaded to '$DOWNLOAD_DEST'."
    echo "You can now inspect 'append.result'."
else
    echo "❌ Error: Failed to 'get' the final file '$HYDFS_FILENAME'."
    exit 1
fi
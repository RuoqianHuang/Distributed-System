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

# --- Main Upload Loop ---

echo "Starting batch file upload..."

# Loop through every argument provided to the script
# "$@" ensures that file paths with spaces are handled correctly
for LOCAL_SOURCE_PATH in "$@"; do
    
    # Check if the provided path is actually a file
    if [ ! -f "$LOCAL_SOURCE_PATH" ]; then
        echo "Warning: File not found, skipping: $LOCAL_SOURCE_PATH"
        continue # Skip to the next argument
    fi
    
    # 1. Determine the destination <filename> on HyDFS
    #    We use 'basename' to get just the file's name (e.g., "file1.txt")
    HYDFS_FILENAME=$(basename "$LOCAL_SOURCE_PATH")
    
    echo "---"
    echo "Uploading Source: $LOCAL_SOURCE_PATH"
    echo "As HyDFS Name:  $HYDFS_FILENAME"
    
    # 2. Execute the create command
    "$CLIENT_BIN" create "$HYDFS_FILENAME" "$LOCAL_SOURCE_PATH"
    
    # 3. Check the exit status of the command
    if [ $? -eq 0 ]; then
        echo "Success: Upload complete."
    else
        echo "Error: Failed to upload '$LOCAL_SOURCE_PATH'."
        # If you want the script to stop on the first error,
        # uncomment the next line:
        # exit 1
    fi
done

echo "---"
echo "All files processed."
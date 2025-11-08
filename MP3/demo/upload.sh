#!/bin/bash

SOURCE_DIR="archive"
CLIENT_EXEC="../bin/client"

echo "Starting upload from '$SOURCE_DIR'..."

find "$SOURCE_DIR" -type f -print0 | while IFS= read -r -d '' LOCAL_SOURCE_PATH; do
    

    #    $LOCAL_SOURCE_PATH = "archive/subfolder/<filename>.txt"
    #    $SOURCE_DIR/       = "my_files/"
    #    $HYDFS_FILENAME    = "subfolder/image.png"
    
    # remove $SOURCE_DIR/ prefix
    HYDFS_FILENAME="${LOCAL_SOURCE_PATH#$SOURCE_DIR/}"
    
    echo "---"
    echo "Uploading Local Path: $LOCAL_SOURCE_PATH"
    echo "As HyDFS Filename:   $HYDFS_FILENAME"

    # upload
    "$CLIENT_EXEC" create "$HYDFS_FILENAME" "$LOCAL_SOURCE_PATH"
    
    # check result
    if [ $? -eq 0 ]; then
        echo "Success: Upload complete."
    else
        echo "Error: Upload failed for '$LOCAL_SOURCE_PATH'"
        exit 1
    fi

done

echo "---"
echo "All files processed."
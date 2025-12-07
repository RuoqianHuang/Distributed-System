#!/bin/bash

HOSTS=(
    "fa25-cs425-b601.cs.illinois.edu"
    "fa25-cs425-b602.cs.illinois.edu"
    "fa25-cs425-b603.cs.illinois.edu"
    "fa25-cs425-b604.cs.illinois.edu"
    "fa25-cs425-b605.cs.illinois.edu"
    "fa25-cs425-b606.cs.illinois.edu"
    "fa25-cs425-b607.cs.illinois.edu"
    "fa25-cs425-b608.cs.illinois.edu"
    "fa25-cs425-b609.cs.illinois.edu"
    "fa25-cs425-b610.cs.illinois.edu"
)



REMOTE_PATH="/cs425/mp4/"
REMOTE_TMP="/tmp/"



echo "--- Starting cleanup on all 10 hosts ---"


for HOST in "${HOSTS[@]}"; do
    echo "Attempting to clean: $HOST"
    
    ssh -T -o ConnectTimeout=5 "$HOST" "rm -rf '$REMOTE_PATH'/worker_rainstorm*; rm -rf '$REMOTE_TMP'/rainstorm-hydfs*"
    
    
    if [ $? -eq 0 ]; then
        echo "✅ Success: $HOST cleaned successfully."
    else
        echo "❌ Failure: Could not clean $HOST. Check connection or permissions."
    fi

    echo "---"

done

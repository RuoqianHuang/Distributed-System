#!/bin/bash

# --- Check if any arguments were provided ---
if [ $# -eq 0 ]; then
    echo "Usage: $0 <hostname1> <hostname2> ..."
    echo "Example: $0 fa25-cs425-b601.cs.illinois.edu fa25-cs425-b602.cs.illinois.edu"
    exit 1
fi

REMOTE_COMMAND="sudo systemctl stop MP3_server.service"

echo "Attempting to stop MP3_server.service on all specified hosts..."
echo "---"

# --- Loop through all hostnames provided as arguments ---
# "$@" correctly handles any arguments with spaces (though unlikely for hostnames)
for HOST in "$@"; do
    echo "Connecting to $HOST..."
    
    # -T disables pseudo-terminal allocation. This is recommended for
    # non-interactive scripts and prevents issues if sudo tries to
    # prompt for a password (which it shouldn't, due to passwordless sudo).
    ssh -T "$HOST" "$REMOTE_COMMAND"
    
    # Check the exit status of the ssh command
    if [ $? -eq 0 ]; then
        echo "✅ Success: Service stopped on $HOST."
    else
        echo "❌ Error: Failed to stop service on $HOST. Check connection or sudo permissions."
    fi
    echo "---"
done

echo "All stop operations complete."
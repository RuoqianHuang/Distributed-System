#!/usr/bin/env python3
import sys

# State: Dictionary to hold running counts
# Key: The column value
# Value: Integer count
counts = {}

def read_tuple():
    try:
        # --- KEY ---
        line_key = sys.stdin.readline()
        if not line_key: return None, None
        
        # FIX: Use rstrip('\n') to preserve the space after 'key:'
        line_key = line_key.rstrip('\n') 
        
        # Split strictly on the protocol separator ": "
        key_parts = line_key.split(": ", 1)
        if len(key_parts) < 2:
            # Fallback: handle case where value might be empty and space was stripped by accident
            # or if the protocol sent "key:" without space
            if ":" in line_key:
                key_parts = line_key.split(":", 1)
            else:
                return None, None # Genuine protocol violation or empty line

        # --- VALUE ---
        line_val = sys.stdin.readline()
        if not line_val: return None, None # Unexpected EOF
        
        # FIX: Use rstrip('\n')
        line_val = line_val.rstrip('\n')
        
        val_parts = line_val.split(": ", 1)
        if len(val_parts) < 2:
            if ":" in line_val:
                val_parts = line_val.split(":", 1)
            else:
                return None, None

        return key_parts[1], val_parts[1]
    except ValueError:
        return None, None

def main():
    # Get column index N. Default to 0 if missing.
    try:
        col_idx = int(sys.argv[1])
    except (IndexError, ValueError):
        col_idx = 0

    while True:
        key, val = read_tuple()
        if key is None: break

        # 1. Parse CSV line
        # Note: Simple split by comma. 
        columns = val.split(',')
        
        # 2. Extract Key (Nth column)
        group_key = ""
        if col_idx < len(columns):
            group_key = columns[col_idx].strip()
        else:
            # "Treat missing data as an empty string - a key of its own"
            group_key = ""

        # 3. Update Count
        current_count = counts.get(group_key, 0) + 1
        counts[group_key] = current_count

        # 4. Emit Running Count
        # The key becomes the group (e.g., "Sign Post"), value is the count
        print("forward", flush=True)
        print(f"key: {group_key}", flush=True)
        print(f"value: {current_count}", flush=True)

if __name__ == "__main__":
    main()
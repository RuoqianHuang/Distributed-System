#!/usr/bin/env python3
import sys, csv

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

def parse_csv(line, col_idx):
    columns = []
    try: 
        reader = csv.reader([line], skipinitialspace=True)
        for row in reader:
            columns = row
            break
    
    except csv.Error:
        return None
    
    if col_idx < len(columns):
        return columns[col_idx]
    return ""

def main():
    # Get pattern from args. Default to empty string if missing.
    # The demo instruction says "pattern A/B will be provided"
    args = sys.argv[1] if len(sys.argv) > 1 else ",0"
    pattern, col_str = args.split(",")
    col_idx = 0
    try:
        col_idx = int(col_str)
    except:
        pass

    while True:
        key, val = read_tuple()
        if key is None: break

        # Check: Case-sensitive string matching
        if pattern in val:

            new_key = parse_csv(val, col_idx)

            print("forward", flush=True)
            print(f"key: {new_key}", flush=True)
            print(f"value: {val}", flush=True)
        else:
            print("filter", flush=True)

if __name__ == "__main__":
    main()
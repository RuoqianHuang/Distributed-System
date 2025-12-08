#!/usr/bin/env python3
import sys

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
    # Get pattern from args. Default to empty string if missing.
    # The demo instruction says "pattern A/B will be provided"
    args = sys.argv[1] if len(sys.argv) > 1 else ","
    pattern, new_pattern = args.split(",")
    

    while True:
        key, val = read_tuple()
        if key is None: break

        print("forward", flush=True)
        print(f"key: {key}", flush=True)
        print(f"value: {val.replace(pattern, new_pattern)}", flush=True)

if __name__ == "__main__":
    main()
# Interactive Client Usage Guide

## Building the Interactive Client

First, build the interactive client:

```bash
make interactive
```

This will create `./bin/interactive`.

## Running the Interactive Client

### Basic Usage (Localhost)

```bash
./bin/interactive
```


## Navigation Guide

### Main Menu
When you start the client, you'll see:
- **members** - View all cluster members (nodes)
- **files** - View all files in the distributed file system
- **exit** - Quit the application

**Controls:**
- **← →** : Navigate between options (members, files, exit)
- **Enter** : Select the highlighted option
- **Escape** : Exit

### File Download
If you select "download":
1. You'll be prompted: "Please enter local filename:"
2. The file will be downloaded using the `get` command
3. You'll see a success/error message

### File Details (Blocks View)
If you select "detail" for a file, you'll see:
- A table of all blocks in that file

### Block Replicas View
If you select "detail" for a block, you'll see:
- A table of all replica nodes storing this block
- Each replica shows: Node ID, Hostname, Port, etc.

### Block Download
If you select "download" for a block:
1. You'll be prompted: "Please enter local filename:"
2. The block will be downloaded directly from the selected replica
3. You'll see a success/error message
4. The application will exit after download

## Example Workflow

1. **Start the client:**
   ```bash
   ./bin/interactive
   ```

2. **View files:**
   - Use **→** to select "files"
   - Press **Enter**

3. **Browse files:**
   - Use **↑ ↓** to navigate through files
   - Use **→** to select "detail" to see blocks
   - Press **Enter**

4. **View blocks:**
   - Use **↑ ↓** to select a block
   - Use **→** to select "detail" to see replicas
   - Press **Enter**

5. **View replicas:**
   - Use **↑ ↓** to select a replica
   - Use **→** to select "download" to download the block
   - Press **Enter**

6. **Download:**
   - Enter local filename when prompted
   - File/block will be downloaded
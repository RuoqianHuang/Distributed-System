# MP3 Demo Guide

This guide provides step-by-step instructions for the MP3 demo tests. Note that some command names may differ from the specification, but the functionality is implemented.

## Prerequisites

- All VMs should be running the MP3 server
- Dataset files should be available locally on VMs

## Command Reference

| Specification Command | Actual Command | Notes |
|----------------------|----------------|-------|
| `create` | `./bin/client create <HyDFSfilename> <localfilepath>` | Create file |
| `get` | `./bin/client get <HyDFSfilename> <localfilepath>` | Download file |
| `append` | `./bin/client append <HyDFSfilename> <localfilepath>` | Append to file |
| `ls HyDFSfilename` | `./bin/client ls <HyDFSfilename>` | List file info and replicas |
| `list_mem_ids` | `./bin/client member` | Shows membership with sorted IDs |
| `liststore` | `./bin/client files` or use `./bin/interactive` → select member → "block" | Shows stored blocks on a VM |
| `merge` | `./bin/client merge <HyDFSfilename>` | Merge file |
| `multiappend` | `./bin/client multiappend <HyDFSfilename> <VM1> <local1> <VM2> <local2> ...` | Concurrent appends |
| `getfromreplica` | `./bin/client getfromreplica <VMaddress> <HyDFSfilename> <blockNumber> <localfilepath>` | Get from specific replica |

---

## Test 1: Create [5%]

**Objective**: Create 5 files sequentially from the dataset.

### Steps:

1. **Choose a VM** (any VM, e.g., VM1: `fa25-cs425-b601.cs.illinois.edu`)

2. **Create 5 files one after another**:
   ```bash
   # On chosen VM (e.g., VM1)
   ssh fa25-cs425-b601.cs.illinois.edu
   
   # Create files (TA will specify which files from dataset)
   ./bin/client create HyDFS/file1.txt /path/to/dataset/file1.txt
   ./bin/client create HyDFS/file2.txt /path/to/dataset/file2.txt
   ./bin/client create HyDFS/file3.txt /path/to/dataset/file3.txt
   ./bin/client create HyDFS/file4.txt /path/to/dataset/file4.txt
   ./bin/client create HyDFS/file5.txt /path/to/dataset/file5.txt
   ```

3. **Wait for background writes to complete** (wait 5-10 seconds after all creates are acknowledged)

4. **Verify creates succeeded**: Each create should return "File created successfully! Replicated to: ..."

---

## Test 2: Get + Correct Replicas on the Ring [10%]

### Check 1: Get [5%]

**Objective**: Get a file and verify it matches the original.

1. **TA picks one file** from the 5 created files (e.g., `HyDFS/file1.txt`)

2. **TA picks a reader VM** (different from writer VM, e.g., VM2)

3. **Get the file**:
   ```bash
   # On reader VM (e.g., VM2)
   ssh fa25-cs425-b602.cs.illinois.edu
   
   ./bin/client get HyDFS/file1.txt /tmp/fetched_file1.txt
   ```

4. **Display the fetched file**:
   ```bash
   # Show file contents
   less /tmp/fetched_file1.txt
   # or
   tail /tmp/fetched_file1.txt
   ```

5. **Compare with original**:
   ```bash
   # On the VM where original dataset file is stored
   diff /path/to/dataset/file1.txt /tmp/fetched_file1.txt
   # Should show no differences (empty output)
   ```

### Check 2: Correct Replicas on the Ring [5%]

**Objective**: Verify file is replicated on correct nodes.

1. **Get fileID and replicas**:
   ```bash
   # On any VM
   ./bin/client ls HyDFS/file1.txt
   ```
   Output shows:
   - FileName and FileID
   - List of replica nodes (hostname:port)

2. **Get sorted membership list**:
   ```bash
   # On any VM, sorted by id
   ./bin/client list_mem_ids 
   ```
   This shows the membership table with IDs. Extract the sorted IDs from the table.

3. **Verify replica placement**:
   - Note the FileID from `ls` output
   - Check that replicas are on the first `n` successors of FileID on the ring
   - Replication factor `n` is 3
   - Verify: FileID should be replicated on nodes with IDs >= FileID (wrapping around)

4. **Show file stored on a VM** (TA picks VM):
   ```bash
   ssh fa25-cs425-b602.cs.illinois.edu
   ./bin/client liststore VMAddress

   # or use the interactive cli
   ./bin/interactive
   ```
---

## Test 3: Post-Failure Re-replication [5%]

**Objective**: Verify re-replication after node failures.

1. **Fail 2 VMs** (at least one replica of the file, TA picks VMs):
   ```bash
   # On the VMs to fail (e.g., VM4 and VM5)
   ssh -T fa25-cs425-b604.cs.illinois.edu "sudo systemctl stop MP3_server"
   ssh -T fa25-cs425-b605.cs.illinois.edu "sudo systemctl stop MP3_server"

   # Rejoin: ssh fa25-cs425-b604.cs.illinois.edu "sudo systemctl start MP3_server"

   ```

2. **Wait for failure detection and re-replication** (wait 10-15 seconds)

3. **Repeat Test 2 Check 2**:
   ```bash
   # Check file replicas again
   ./bin/client ls HyDFS/file1.txt
   
   # Check membership (should show failed nodes)
   ./bin/client member
   
   # Verify file is now on correct new replicas
   # Check that replicas are on the first n successors of FileID
   ```

4. **Verify file stored on new replica VM**:
   ```bash
   # On one of the new replica VMs (TA picks)
   ssh fa25-cs425-b602.cs.illinois.edu
   ./bin/client files
   ```
---

## Test 4: Read-My-Writes + Client-Append Ordering [15%]

**Objective**: Verify read-my-writes consistency and append ordering.

### Setup:
- **Pick a VM** (e.g., VM1)
- **Pick two files from dataset** (e.g., `foo1.txt`, `foo2.txt`)

### Appends [5%]:

1. **Append first file**:
   ```bash
   # On chosen VM (e.g., VM1)
   ssh fa25-cs425-b601.cs.illinois.edu
   # Assume append to HyDFS/foo.txt
   ./bin/client append HyDFS/foo.txt /path/to/dataset/foo1.txt
   # Wait for create to complete
   ```

2. **Append second file immediately after first completes**:
   ```bash
   ./bin/client append HyDFS/foo.txt /path/to/dataset/foo2.txt
   ```

### Check for Read-My-Writes and Ordering [10%]:

**Check 1: Read-My-Writes [5%]**

1. **Get the file from the same VM**:
   ```bash
   # Still on VM1
   ./bin/client get HyDFS/foo.txt /tmp/fetched_foo.txt
   ```

2. **Verify both appends are present**:
   ```bash
   # TA will use grep to check for keywords
   grep "keyword_from_foo1" /tmp/fetched_foo.txt
   grep "keyword_from_foo2" /tmp/fetched_foo.txt
   # Both should find matches
   ```

**Check 2: Append Ordering [5%]**

1. **Verify correct order**:
   ```bash
   # TA will grep to verify ordering
   # foo1.txt content should appear before foo2.txt content
   grep -n "keyword_from_foo1" /tmp/fetched_foo.txt
   grep -n "keyword_from_foo2" /tmp/fetched_foo.txt
   # Line numbers should show foo1 before foo2
   ```

---

## Test 5: Concurrent Appends + Merge [15%]

**Objective**: Verify concurrent appends and merge functionality.

### Setup:
- Use the same file: `HyDFS/foo.txt` (from Test 4)
- Choose 4 VMs for concurrent appends (e.g., VM1, VM2, VM3, VM4)
- TA will pick files for each VM

### Check 1: Append Concurrency [5%]

1. **Launch multiappend** (from one VM, e.g., VM1):
   ```bash
   # On VM1
   ssh fa25-cs425-b601.cs.illinois.edu

   ./bin/client multiappend HyDFS/foo.txt \
     fa25-cs425-b601.cs.illinois.edu /path/to/dataset/file_for_vm1.txt \
     fa25-cs425-b602.cs.illinois.edu /path/to/dataset/file_for_vm2.txt \
     fa25-cs425-b603.cs.illinois.edu /path/to/dataset/file_for_vm3.txt \
     fa25-cs425-b604.cs.illinois.edu /path/to/dataset/file_for_vm4.txt
   ```

2. **Show concurrent appends at replicas**:
   - Use MP1's distributed querier to search logs across all replica VMs
   - Look for print statements showing:
     - `received write request ...`
     - `completed writing ...`
   - Verify that all 4 appends show overlapping timestamps, indicating concurrency

   ```bash
   # Using MP1 querier to search across all machines
   # Navigate to MP1 directory
   cd ../MP1
   
   # TODO
   
   ```

3. **Wait for all appends to complete**

### After Merge [10%]

1. **Run merge**:
   ```bash
   # On any VM
   ./bin/client merge HyDFS/foo.txt
   ```

2. **Wait for merge to complete** (10-20 seconds)

**Check 1: Replicas are Identical [5%]**

1. **Get file from two different replicas**:
   ```bash
   # First, find replicas
   ./bin/client ls HyDFS/foo.txt
   # Note two replica addresses (e.g., VM6, VM7)
   
   # Get block 0 from first replica
   ./bin/client getfromreplica fa25-cs425-b606.cs.illinois.edu HyDFS/foo.txt 0 /tmp/replica1_block0.txt
   
   # Get block 0 from second replica
   ./bin/client getfromreplica fa25-cs425-b607.cs.illinois.edu HyDFS/foo.txt 0 /tmp/replica2_block0.txt
   ```

2. **Compare the blocks**:
   ```bash
   diff /tmp/replica1_block0.txt /tmp/replica2_block0.txt
   # Should show no differences
   ```

   **Note**: If file has multiple blocks, repeat for all blocks or get the full file using `get` command from both replicas.

**Check 2: No Appends Lost [5%]**

1. **Get the merged file**:
   ```bash
   ./bin/client get HyDFS/foo.txt /tmp/merged_foo.txt
   ```

2. **Verify all 4 appends are present**:
   ```bash
   # TA will grep for keywords from all 4 appended files
   grep "keyword_from_vm1_file" /tmp/merged_foo.txt
   grep "keyword_from_vm2_file" /tmp/merged_foo.txt
   grep "keyword_from_vm3_file" /tmp/merged_foo.txt
   grep "keyword_from_vm4_file" /tmp/merged_foo.txt
   # All should find matches
   ```

---

## Troubleshooting

### Useful Commands:

- **Check status**: `./bin/client status`
- **View interactive**: `./bin/interactive` (for browsing members, files, blocks)

---

## Notes

- Replication factor: 3
- Quorum: W=2 (write), R=2 (read)


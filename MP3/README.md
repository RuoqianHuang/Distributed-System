# Machine Programming 3 – Hybrid Distributed File System

## How to build server and client binary
All binaries will be compiled into the bin/ directory.

````
# Build the client 
go build -o bin/client ./cmd/client

# Build the interactive file viewer 
go build -o bin/interactive ./cmd/interactive

# Build the server
go build -o bin/server ./cmd/server


````
## Deploy guide
This project uses Ansible for automated deployment and management via ``systemd`` services.

### Prerequisite
1. All remote servers installed with Python and ssh server.
2. Setup ssh key and passwordless sudo on remote servers.
3. Install ansible-core on your local machine.
4. The working directory must be created on all servers: ``mkdir -p /cs425/mp3``

### Deployment Steps
1. Build the server binary with ``make server``.
2. Deploy server binary with ``ansible-playbook -i inventory.ini install-playbook.yml``
3. To uninstall server binary, run ``ansible-playbook -i inventory.ini uninstall-playbook.yml``


### Deployment Notes
- Working Directory: All sharded file blocks are stored at ``/cs425/mp3``. To change this location, modify the ``WorkingDirectory`` in ``templates/service.j2``.
- Cleanup: You can use the ``cleanup.sh`` script (requires SSH access) to empty the working directory on all nodes.
- Troubleshooting: If deployment fails, first check that the hostnames in ``inventory.ini`` are correct and that you have sufficient permissions for the working directory and ``systemd`` services.
- Performance Tuning: You can tune the number of concurrent workers by changing the ``NumOf...Workers`` parameters in the ``DistributedFiles`` object in ``cmd/server/main.go``.

## Client Usage
1. CLI client 
The ``./bin/client`` is suitable for scripting or single commands.
Usage: ``./bin/client <command> [args...]``

- ``./bin/client status``: Displays the status and membership list of the connected node.

- ``./bin/client files``: Displays metadata for all files in the cluster.


- ``./bin/client create <HyDFS_Filename> <Local_SourcePath>``: Uploads a new file.


- ``./bin/client append <HyDFS_Filename> <Local_SourcePath>``: Appends data to an existing file.


- ``./bin/client get <HyDFS_Filename> <Local_DestPath>``: Downloads a file.


- ``./bin/client ls <HyDFS_Filename>``: Lists all VMs where the file is currently stored.

2. Interactive TUI Client

The interactive client provides a full-screen TUI and is the recommended way to interact with HyDFS.

Usage: ``./bin/interactive``

- Navigation: Use Left/Right arrows to select an Action (e.g., Download, Info). Use Up/Down arrows to select a file from the table.
- Confirm: Use the Enter key to execute the selected action on the selected file.

## System Architecture

### Failure detector 
- The system uses a Gossip-based protocol (from MP2) to maintain a cluster membership list.
- To tolerate 2 simultaneous failures, the cluster has 3 designated Leader (Introducer) nodes.
- The Leaders gossip among themselves to prevent network partitions.
- If a non-leader node loses contact with all Leaders, it will restart and rejoin the cluster with a new ID.


### Local Storage
- The FileManager (``/internal/files/files.go``) provides APIs for local file operations on each node.
- Files in HyDFS are split into two parts: 
    - Metadata (Meta): Contains the file name, file size, block list, and a Counter (version number).
    - Blocks: The file's actual content is sharded into blocks and stored on different nodes.


### Write Operation (Create / Append)
Write operations use a variant of Two-Phase Commit (2PC), combined with **Quorum (W=2)** and a Coordinator-based Lock, to ensure consistency.

1. Phase 1: Pre-write (Buffer)
- The client acquires a write lock from the coordinator (typically the first replica node for the file ID).
- The client sends all new file blocks to the $N$ replica nodes.
- Each replica stores the incoming block in a temporary buffer (``BufferedBlockMap``) but does not commit it to the file system.
- The client waits for at least $W=2$ nodes to acknowledge the buffer.


2. Phase 2: Commit (Update Meta)
- The client update the Metadata objects (with an incremented ``Counter`` version) to all replica nodes.
- The client releases the lock.

3. Background Commit
- The ``workerLoopBuffered`` goroutine on each node periodically checks its buffered blocks.


### Read Operation (Get)

Read operations use a **Read Quorum (R=2)** to ensure the latest version is retrieved, satisfying Read-My-Writes.

1. Get Meta: The client requests the file's metadata from all $N$ replicas (``getRemoteMeta``).

2. Wait for Quorum: The client waits for $R=2$ responses.

3. Select Version: The client selects the Metadata with the highest ``Counter`` (version) from the $R$ responses.

4. Get Blocks: Using this latest metadata, the client fetches all required file blocks in parallel (also using an $R=2$ quorum via ``getRemoteBlock``).

5. Reassemble: The client reassembles the blocks in order locally to create the final file.

### Merge 

``Merge`` is a background process that ensures Eventual Consistency and repairs failed writes or node failures. It can also be invoked by a client to ensure that a certain file is fully replicated.


- The ``jobCreator`` goroutine periodically triggers a Merge (e.g., every 5 seconds).
- The ``Merge`` function pushes all locally stored ``meta`` and ``block`` IDs into their respective work queues (using a map to prevent duplicates).
- The ``workerLoopMeta`` and ``workerLoopBlock`` goroutines consume these jobs. They check their lists of "alive" members (from the Failure Detector) and send their local data (sendMeta, sendBlock) to the file's assigned replicas.
- The receiving node compares the ``Counter`` (version). If its local version is older, it accepts and updates its data. This ensures that data is re-replicated after failures and that all replicas eventually converge to the same state.
# MP4 - RainStorm Stream Processing


RainStorm is a distributed, fault-tolerant stream processing framework (similar to Apache Storm) built on top of HyDFS (MP3). It supports defining multi-stage processing topologies, executing arbitrary user-defined operators (in Python), and guarantees exactly-once semantics even in the presence of node failures.



## How to build


All binaries will be compiled into the ``bin/`` directory.

```bash
# Build the main RainStorm Leader binary
go build -o bin/rainstorm ./cmd/rainstorm

# Build the Worker binary (deployed to remote nodes)
go build -o bin/worker ./cmd/worker

```


## Deploy Guide

### Prerequisites

1. HyDFS Cluster: The HyDFS system (MP3) must be running on all nodes (fa25-cs425-b601 to b610). RainStorm relies on HyDFS for logging (WAL) and storage.

2. SSH Access: The machine running the Leader must have passwordless SSH access to all other VMs to spawn workers.

3. Operator Scripts: operator scripts (e.g., op_filter.py) must be available locally on the Leader machine.


### Execution


RainStorm is triggered by running the ``rainstorm`` binary on the Leader node. The Leader automatically provisions workers on remote nodes via SSH/SCP.


### Command Syntax:

```bash
./bin/rainstorm <WorkerBinPath> <NStages> <NTasks> \
    <Op1_Exe> <Op1_Args> <Op1_Type> ... \
    <N_SRC_FILES> <HydfsSrcFile_1> ... \ 
    <AutoScalable> <InputRate> <LW> <HW> \
    <HydfsDestFile> <ExactlyOnce>
```

Example: Please refer to scripts in ``./demo``


### Operator Arguments
- Op_Exe: Path to the executable (e.g., op_filter.py).
- Op_Args: Arguments passed to the script (e.g., regex pattern).
- Op_Type: One of filter, transform, aggregate. NOTE: aggregate stages are stateful and will not be autoscaled.


## System Architecture

### Leader (The Coordinator)

The Leader is the central control plane for the topology.

- Scheduler: Assigns tasks to available VMs using a Round-Robin strategy.

- Process Management: Uses ssh to spawn remote worker processes and scp to deploy binaries/operators.

- Resource Monitor: A background thread that monitors throughput (FlowCounter). It triggers Autoscaling (up/down) if the average tuple throughput crosses the Low Watermark (LW) or High Watermark (HW).

- Failure Recovery: Integrated with the MP2 Failure Detector. If a worker node fails, the Leader detects it and re-spawns the task on a healthy node.


### Worker (The Processing Unit)

Workers execute the actual stream processing logic.

- Operator Runner: A dedicated component that wraps the user-defined Python script. It communicates via stdin/stdout using a strict line-based protocol (key: ... \n value: ...).

- Input/Output: Receives tuples via RPC HandleTuple. Forwards processed tuples to the next stage using Hash Partitioning on the tuple Key.

- Buffering: Uses a thread-safe Queue with blocking semantics to handle bursty traffic without deadlocking.


### Exactly-Once Semantics (Reliability)
RainStorm guarantees that every tuple is processed exactly once, reflected in the final output.

- Tuple ID: Every tuple is assigned a unique global ID (e.g., src:0, src:0-1).

- Deduplication: Workers maintain an in-memory map (AckedTuple) of processed IDs. Duplicate tuples caused by upstream retries are dropped and immediately ACKed.

- Write-Ahead Log (WAL): Before acknowledging a tuple to the upstream sender, the worker writes a PROCESSED,<TupleID> entry to a log file in HyDFS.

- Synchronous Forwarding: A tuple is not considered "done" until it has been successfully forwarded to (and acknowledged by) the next stage (or written to HyDFS for the last stage).

- Recovery: On restart, a worker reads its specific HyDFS log file to rebuild its AckedTuple state, preventing double-processing.


### Autoscaling

- Metrics: Workers report their flow rate (tuples/sec) to the Leader via the Failure Detector gossip or RPC.

- Cooldown: To prevent oscillation (rapidly scaling up and down), the Leader enforces a cooldown period (e.g., 10s) between scaling events on the same stage.

- Stateful Protection: The Autoscaler explicitly ignores stages marked as aggregate to preserve state consistency.

### Distributed File System Integration (HyDFS)

- Input: The Leader downloads the source file from HyDFS, splits it into lines, and streams them as tuples to Stage 1.

- Output: The final stage workers append results directly to the destination file in HyDFS.

- replay: Every worker maintains a replay file in HyDFS (task-stage-id.log) for fault tolerance. We use a buffered HyDFS client to batch small appends for performance.


## Client/Operator Development

Users can write custom operators in Python. The protocol is simple:

#### Input (from Stdin):
````
key: <key_content>
value: <value_content>
````

#### Output (to Stdout):
````
forward
key: <new_key>
value: <new_value>
````
OR
````
filter
````


## Troubleshooting

1. "Protocol Mismatch" / "Process Closed Stdout": This usually means the Python operator crashed. Check worker_*.log on the remote machine; our OperatorRunner captures and logs the Python stderr traceback.

2. Worker Deadlock: If the system stalls, check if the input_rate is too high for the cluster capacity. The internal queues might be full.

3. HyDFS Errors: Ensure the HyDFS service is running and healthy (./bin/client status from MP3) before starting RainStorm.

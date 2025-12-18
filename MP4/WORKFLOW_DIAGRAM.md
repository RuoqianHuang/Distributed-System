# RainStorm Workflow Diagram

## Complete System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              LEADER (Stage 0)                                │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Data Structures:                                                       │  │
│  │   WaitedTuples: map[string]string  // ID → Value (unacked)           │  │
│  │   DoneTuples:   map[string]bool    // ID → true (completed)          │  │
│  │   NumTasksStages: []int            // Tasks per stage                 │  │
│  │   VMWorkerCount: map[string]int    // VM → worker count               │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Source Process (DoTask)                                               │  │
│  │   1. Download file from HyDFS                                         │  │
│  │   2. For each line:                                                   │  │
│  │      - Create Tuple{ID, Key, Value}                                   │  │
│  │      - WaitedTuples[ID] = Value  // Track unacked                    │  │
│  │      - resendQueue.Push(ID)  // Add to retry queue                    │  │
│  │      - sendTuple() → Stage 1 (hash partition)                         │  │
│  │   3. Continuous retry loop:                                           │  │
│  │      - Pop from resendQueue                                           │  │
│  │      - If in WaitedTuples: resend and push back to queue              │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ RPC Handlers:                                                          │  │
│  │   HandleAck(id) → delete(WaitedTuples, id)                            │  │
│  │   HandleResult(tuple) → DoneTuples[ID]=true, write to HyDFS          │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ ResourceMonitor (every 250ms):                                        │  │
│  │   - Check membership for failed workers                                │  │
│  │   - Restart failed tasks                                               │  │
│  │   - Autoscale based on flow rates                                      │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Tuples (RPC)
                                    │ SourceHost=Leader, SourceRPCPort=9001
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          STAGE 1 WORKERS                                     │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐         │
│  │   Worker 1-0     │  │   Worker 1-1     │  │   Worker 1-N     │         │
│  │  (Task-1-0)      │  │  (Task-1-1)      │  │  (Task-1-N)      │         │
│  ├──────────────────┤  ├──────────────────┤  ├──────────────────┤         │
│  │ Data Structures: │  │ Data Structures: │  │ Data Structures: │         │
│  │ AckedTuple:      │  │ AckedTuple:      │  │ AckedTuple:      │         │
│  │   map[string]bool│  │   map[string]bool│  │   map[string]bool│         │
│  │ ProcessedTuple:  │  │ ProcessedTuple:  │  │ ProcessedTuple:  │         │
│  │   map[string]    │  │   map[string]    │  │   map[string]    │         │
│  │     Tuple        │  │     Tuple        │  │     Tuple        │         │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘         │
│         │                      │                      │                     │
│         │ Operator:           │ Operator:           │ Operator:            │
│         │   grep_op           │   grep_op           │   grep_op            │
│         │   (filter)          │   (filter)          │   (filter)           │
│         │                     │                     │                      │
│         │ Process:            │ Process:            │ Process:             │
│         │   1. Receive tuple  │   1. Receive tuple  │   1. Receive tuple   │
│         │      → Queue        │      → Queue        │      → Queue         │
│         │   2. Pop from queue │   2. Pop from queue │   2. Pop from queue  │
│         │   3. Check          │   3. Check          │   3. Check           │
│         │      AckedTuple     │      AckedTuple     │      AckedTuple      │
│         │      (deduplication)│      (deduplication)│      (deduplication) │
│         │   4. Process via    │   4. Process via    │   4. Process via     │
│         │      operator       │      operator       │      operator        │
│         │   5. Log to HyDFS   │   5. Log to HyDFS   │   5. Log to HyDFS    │
│         │   6. Send ack       │   6. Send ack       │   6. Send ack        │
│         │      upstream       │      upstream       │      upstream        │
│         │   7. Forward to     │   7. Forward to     │   7. Forward to      │
│         │      Stage 2        │      Stage 2        │      Stage 2         │
│         │                     │                     │                      │
│         └─────────┬───────────┴──────────┬──────────┘                      │
│                   │                      │                                 │
│                   │ Tuples (RPC)         │                                 │
│                   │ SourceHost=Worker,   │                                 │
│                   │ SourceRPCPort=6667   │                                 │
│                   ▼                      ▼                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Hash Partition by Key
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          STAGE 2 WORKERS                                     │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐         │
│  │   Worker 2-0     │  │   Worker 2-1     │  │   Worker 2-N     │         │
│  │  (Task-2-0)      │  │  (Task-2-1)      │  │  (Task-2-N)      │         │
│  ├──────────────────┤  ├──────────────────┤  ├──────────────────┤         │
│  │ Data Structures: │  │ Data Structures: │  │ Data Structures: │         │
│  │ AckedTuple:      │  │ AckedTuple:      │  │ AckedTuple:      │         │
│  │   map[string]bool│  │   map[string]bool│  │   map[string]bool│         │
│  │ processingQueue: │  │ processingQueue: │  │ processingQueue: │         │
│  │   *queue.Queue   │  │   *queue.Queue   │  │   *queue.Queue   │         │
│  │ inProcessingQueue│  │ inProcessingQueue│  │ inProcessingQueue│         │
│  │   map[string]bool│  │   map[string]bool│  │   map[string]bool│         │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘         │
│         │                      │                      │                     │
│         │ Operator:           │ Operator:           │ Operator:            │
│         │   replace_op        │   replace_op        │   replace_op         │
│         │   (transform)       │   (transform)       │   (transform)        │
│         │                     │                     │                      │
│         │ Process:            │ Process:            │ Process:             │
│         │   1. Receive tuple  │   1. Receive tuple  │   1. Receive tuple   │
│         │      → Queue        │      → Queue        │      → Queue         │
│         │   2. Pop from queue │   2. Pop from queue │   2. Pop from queue  │
│         │   3. Check          │   3. Check          │   3. Check           │
│         │      AckedTuple     │      AckedTuple     │      AckedTuple      │
│         │   4. Process via    │   4. Process via    │   4. Process via     │
│         │      operator       │      operator       │      operator        │
│         │   5. Log to HyDFS   │   5. Log to HyDFS   │   5. Log to HyDFS    │
│         │   6. Send ack       │   6. Send ack       │   6. Send ack        │
│         │      upstream       │      upstream       │      upstream        │
│         │   7. Send result    │   7. Send result    │   7. Send result     │
│         │      to Leader      │      to Leader      │      to Leader       │
│         │                     │                     │                      │
│         └─────────┬───────────┴──────────┬──────────┘                      │
│                   │                      │                                 │
│                   │ Results (RPC)        │                                 │
│                   │ HandleResult()       │                                 │
│                   ▼                      ▼                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │
                                    ▼
                            Leader (Output)
                            - DoneTuples[ID] = true
                            - Write to Console
                            - Append to HyDFS
```

## Detailed Tuple Flow with Acks

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              LEADER (Source)                                 │
│                                                                               │
│  WaitedTuples: {                                                             │
│    "file:0" → "hello world",                                                 │
│    "file:1" → "foo bar",                                                     │
│    "file:2" → "hello foo"                                                    │
│  }                                                                            │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Step 1: Send Tuple                                                  │   │
│  │   Tuple{                                                             │   │
│  │     ID: "file:0",                                                    │   │
│  │     Key: "file:0",                                                   │   │
│  │     Value: "hello world",                                            │   │
│  │     SourceHost: "leader",                                            │   │
│  │     SourceRPCPort: 9001                                              │   │
│  │   }                                                                  │   │
│  │   → Hash("file:0") % 3 = 0 → Worker 1-0                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    │ RPC: Worker.HandleTuple()               │
│                                    ▼                                         │
┌─────────────────────────────────────────────────────────────────────────────┐
│                          WORKER 1-0 (Stage 1)                                │
│                                                                               │
│  AckedTuple: {}  (empty initially)                                           │
│  processingQueue: []                                                         │
│  inProcessingQueue: {}                                                       │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Step 2: Receive & Process                                           │   │
│  │   1. HandleTuple() → Push to processingQueue                        │   │
│  │   2. Pop from queue                                                 │   │
│  │   3. Check AckedTuple["file:0"] → false (not processed)             │   │
│  │   4. Process via operator:                                          │   │
│  │      Input:  "key: file:0\nvalue: hello world\n"                    │   │
│  │      Output: "forward\nkey: file:0\nvalue: hello world\n"           │   │
│  │   5. AckedTuple["file:0"] = true                                     │   │
│  │   6. Log to HyDFS: "PROCESSED,file:0"                                │   │
│  │   7. Send ack upstream:                                              │   │
│  │      RPC: Leader.HandleAck("file:0")                                 │   │
│  │   8. Forward to Stage 2:                                             │   │
│  │      Hash("file:0") % 3 = 0 → Worker 2-0                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    │ RPC: Worker.HandleTuple()               │
│                                    ▼                                         │
┌─────────────────────────────────────────────────────────────────────────────┐
│                          WORKER 2-0 (Stage 2)                                │
│                                                                               │
│  AckedTuple: {}                                                              │
│  processingQueue: []                                                         │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Step 3: Receive & Process                                           │   │
│  │   1. HandleTuple() → Push to processingQueue                        │   │
│  │   2. Pop from queue                                                 │   │
│  │   3. Check AckedTuple["file:0"] → false                             │   │
│  │   4. Process via operator:                                          │   │
│  │      Input:  "key: file:0\nvalue: hello world\n"                    │   │
│  │      Output: "forward\nkey: file:0\nvalue: hi world\n"              │   │
│  │   5. AckedTuple["file:0"] = true                                     │   │
│  │   6. Log to HyDFS: "PROCESSED,file:0"                                │   │
│  │   7. Send ack upstream:                                              │   │
│  │      RPC: Worker.HandleAck("file:0") → ALL Stage 1 workers          │   │
│  │   8. Send result to leader:                                          │   │
│  │      RPC: Leader.HandleResult(Tuple{ID:"file:0", ...})               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    │ RPC: Worker.HandleAck()                 │
│                                    ▼                                         │
┌─────────────────────────────────────────────────────────────────────────────┐
│                          WORKER 1-0 (Stage 1)                                │
│                                                                               │
│  AckedTuple: {"file:0": true}                                                │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Step 4: Receive Ack from Downstream                                 │   │
│  │   1. HandleAck("file:0")                                            │   │
│  │   2. AckedTuple["file:0"] = true (already true)                     │   │
│  │   3. Log to HyDFS: "PROCESSED,file:0" (duplicate, but safe)         │   │
│  │   4. Forward ack upstream:                                          │   │
│  │      Get all upstream workers (Stage 0 = Leader)                    │   │
│  │      RPC: Leader.HandleAck("file:0")                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    │ RPC: Leader.HandleAck()                 │
│                                    ▼                                         │
┌─────────────────────────────────────────────────────────────────────────────┐
│                              LEADER                                          │
│                                                                               │
│  WaitedTuples: {                                                             │
│    "file:1" → "foo bar",                                                     │
│    "file:2" → "hello foo"                                                    │
│  }  // "file:0" removed                                                      │
│                                                                               │
│  DoneTuples: {"file:0": true}                                                │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Step 5: Receive Ack & Result                                        │   │
│  │   1. HandleAck("file:0") → delete(WaitedTuples, "file:0")           │   │
│  │   2. HandleResult(Tuple) → DoneTuples["file:0"] = true              │   │
│  │   3. Write to console: "file:0: hi world"                            │   │
│  │   4. Append to HyDFS: "file:0: hi world\n"                           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Ack Flow Diagram

```
                    UPSTREAM                    DOWNSTREAM
                       │                            │
                       │                            │
    ┌──────────────────┴──────────────────┐        │
    │                                     │        │
    ▼                                     ▼        ▼
┌─────────┐                          ┌─────────┐ ┌─────────┐
│ Leader  │                          │Worker 1 │ │Worker 2 │
│(Source) │                          │(Stage 1)│ │(Stage 2)│
└─────────┘                          └─────────┘ └─────────┘
    │                                     │            │
    │ Tuple                               │            │
    │─────────────────────────────────────>│            │
    │                                     │            │
    │                                     │ Tuple      │
    │                                     │───────────>│
    │                                     │            │
    │                                     │            │ Process
    │                                     │            │ & Log
    │                                     │            │
    │                                     │            │ Ack
    │                                     │<───────────│
    │                                     │            │
    │                                     │ Process    │
    │                                     │ Ack        │
    │                                     │            │
    │ Ack                                 │            │
    │<─────────────────────────────────────│            │
    │                                     │            │
    │                                     │            │ Result
    │ Result                              │            │───────────┐
    │<─────────────────────────────────────┼────────────┼───────────┘
    │                                     │            │
    │                                     │            │
    │ Write to                            │            │
    │ Console & HyDFS                     │            │
    │                                     │            │
```

## Data Structure Details

### Leader Data Structures

```go
type Leader struct {
    // Task Configuration
    TaskName         string              // "rainstorm-2024-01-01T00:00:00Z"
    NumStages        int                 // Number of stages
    NumTasksPerStage int                 // Initial tasks per stage
    NumTasksStages   []int               // Current tasks per stage [3, 3, 3]
    Stages           []OpStage           // Operator configs per stage
    
    // Exactly-Once Tracking
    WaitedTuples     map[string]string   // ID → Value (unacked from source)
    muWaitedTuples   sync.RWMutex        // Lock for WaitedTuples
    DoneTuples       map[string]bool     // ID → true (completed)
    muDoneTuples     sync.RWMutex        // Lock for DoneTuples
    resendQueue      *queue.Queue        // Queue of tuple IDs to retry
    
    // Task Management
    VMWorkerCount    map[string]int      // VM → worker count
    vmRRIndex        int                 // Round-robin index
    
    // Autoscaling
    AutoScale        bool
    InputRate        int                 // Tuples/second
    LowWatermark     int                 // Scale down threshold
    HighWatermark    int                 // Scale up threshold
    
    // Infrastructure
    fd               *detector.FD        // Failure detector
    flow             *flow.FlowCounter   // Flow metrics
}
```

### Worker Data Structures

```go
type Worker struct {
    // Identity
    TaskName         string              // "rainstorm-2024-01-01T00:00:00Z-1-0"
    Stage            int                 // Stage number (1, 2, 3)
    StageID          int                 // Task ID within stage (0, 1, 2)
    
    // Exactly-Once Tracking
    AckedTuple       map[string]bool     // ID → true (processed & acked)
    muAckedTuple     *sync.RWMutex       // Lock for AckedTuple
    
    // Processing Queue
    processingQueue  *queue.Queue        // Queue of tuples to process
    inProcessingQueue map[string]bool    // Track tuples in queue (prevent duplicates)
    muInProcessingQueue *sync.RWMutex    // Lock for inProcessingQueue
    
    // Operator
    runner           *OperatorRunner     // External operator process
    
    // Routing
    isLastStage      bool                // Is this the last stage?
    isNextStageScalable bool             // Can next stage autoscale?
    NumTasksNextStage int                // Tasks in next stage
    
    // Infrastructure
    fd               *detector.FD        // Failure detector
    flow             *flow.FlowCounter   // Flow metrics
}
```

## Hash Partitioning Algorithm

```
┌─────────────────────────────────────────────────────────────────┐
│ Hash Partitioning Logic                                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ Input: Tuple with Key = "file:0"                               │
│                                                                 │
│ Step 1: Hash the key                                           │
│   hash = FNV32("file:0") = 0x12345678                          │
│                                                                 │
│ Step 2: Get target stage workers                               │
│   candidates = GetAliveMembersInStage(nextStage)                │
│   // Returns: {0: Worker1, 1: Worker2, 2: Worker3}             │
│                                                                 │
│ Step 3: Calculate target                                       │
│   IF nextStageScalable:                                        │
│     targetID = hash % len(candidates)  // Dynamic              │
│   ELSE:                                                         │
│     targetID = hash % NumTasksNextStage  // Fixed              │
│                                                                 │
│ Step 4: Select worker                                          │
│   targetWorker = candidates[targetID]                           │
│                                                                 │
│ Example:                                                       │
│   hash = 0x12345678 = 305419896                                │
│   len(candidates) = 3                                          │
│   targetID = 305419896 % 3 = 0                                 │
│   → Send to Worker at stageID=0                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Failure Recovery Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ Normal Operation                                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ Worker 1-0: Running, processing tuples                         │
│   AckedTuple: {"file:0": true, "file:1": true}                 │
│   Log file: "Task-1-0.log"                                     │
│     PROCESSED,file:0                                            │
│     PROCESSED,file:1                                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Process killed
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Failure Detected                                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ ResourceMonitor (every 250ms):                                 │
│   1. Check membership for Stage 1                              │
│   2. Expected: {0, 1, 2}                                       │
│   3. Found: {1, 2}  // Worker 0 missing                        │
│   4. → Restart Worker 1-0                                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Restart
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Worker Restart                                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ New Worker 1-0 (same identity):                                │
│   1. Start operator process                                    │
│   2. RecoverStateFromHyDFS():                                  │
│      a. Download "Task-1-0.log" from HyDFS                     │
│      b. Parse: "PROCESSED,file:0"                              │
│      c. Parse: "PROCESSED,file:1"                              │
│      d. AckedTuple["file:0"] = true                            │
│      e. AckedTuple["file:1"] = true                            │
│   3. Join failure detector                                     │
│   4. Start RPC server                                          │
│   5. Ready to process new tuples                               │
│                                                                 │
│ If duplicate tuple arrives:                                    │
│   - Check AckedTuple → already processed                       │
│   - Send ack (idempotent)                                      │
│   - Don't reprocess                                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Autoscaling Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ ResourceMonitor (every 250ms)                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ FOR each stage (except aggregate):                             │
│   1. Get alive workers: {0, 1, 2}                              │
│   2. Calculate total flow:                                     │
│      total_flow = worker[0].Flow + worker[1].Flow + ...        │
│                                                                 │
│   3. IF total_flow < LowWatermark AND numWorkers < MAX:       │
│      → Scale UP:                                               │
│         - Increment NumTasksStages[stage]                      │
│         - startWorker(stage, newTaskID)                        │
│         - Mark as pending (5s bootup timeout)                  │
│                                                                 │
│   4. IF total_flow > HighWatermark AND numWorkers > 1:        │
│      → Scale DOWN:                                             │
│         - Decrement NumTasksStages[stage]                      │
│         - stopWorker(stage, lastTaskID)                        │
│                                                                 │
│   5. Workers automatically adapt:                              │
│      - getTargetHost() uses alive workers list                 │
│      - New workers join membership                             │
│      - Partitioning automatically adjusts                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Operator Execution Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ OperatorRunner Process                                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ Worker Process          Operator Executable (grep_op)          │
│      │                          │                               │
│      │  "key: file:0\n"         │                               │
│      │─────────────────────────>│                               │
│      │  "value: hello\n"        │                               │
│      │─────────────────────────>│                               │
│      │                          │                               │
│      │                          │  Process:                     │
│      │                          │    - Read key/value           │
│      │                          │    - Apply grep pattern       │
│      │                          │    - Decide: filter/forward   │
│      │                          │                               │
│      │  "forward\n"             │                               │
│      │<─────────────────────────│                               │
│      │  "key: file:0\n"         │                               │
│      │<─────────────────────────│                               │
│      │  "value: hello\n"        │                               │
│      │<─────────────────────────│                               │
│      │                          │                               │
│      │  Parse output            │                               │
│      │  Create output tuple     │                               │
│      │                          │                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Complete End-to-End Example

```
┌─────────────────────────────────────────────────────────────────┐
│ Input File (HyDFS): "input.txt"                                 │
│   Line 0: "hello world"                                         │
│   Line 1: "foo bar"                                             │
│   Line 2: "hello foo"                                           │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Download
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Leader: StartTask()                                             │
│   WaitedTuples: {}                                              │
│                                                                 │
│   Tuple 0: ID="input.txt:0", Key="input.txt:0", Value="hello"  │
│     WaitedTuples["input.txt:0"] = "hello"                      │
│     → Hash("input.txt:0") % 3 = 0 → Worker 1-0                 │
│                                                                 │
│   Tuple 1: ID="input.txt:1", Key="input.txt:1", Value="foo"    │
│     WaitedTuples["input.txt:1"] = "foo"                        │
│     → Hash("input.txt:1") % 3 = 1 → Worker 1-1                 │
│                                                                 │
│   Tuple 2: ID="input.txt:2", Key="input.txt:2", Value="hello"  │
│     WaitedTuples["input.txt:2"] = "hello"                      │
│     → Hash("input.txt:2") % 3 = 2 → Worker 1-2                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Process in parallel
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Stage 1: Grep (pattern="hello")                                 │
│                                                                 │
│ Worker 1-0: "input.txt:0" → "hello" → FORWARD                  │
│   AckedTuple["input.txt:0"] = true                             │
│   Log: "PROCESSED,input.txt:0"                                 │
│   Ack → Leader                                                  │
│   Forward → Stage 2 Worker 0                                    │
│                                                                 │
│ Worker 1-1: "input.txt:1" → "foo" → FILTER                    │
│   AckedTuple["input.txt:1"] = true                             │
│   Log: "PROCESSED,input.txt:1"                                 │
│   Ack → Leader                                                  │
│   (No forward - filtered)                                       │
│                                                                 │
│ Worker 1-2: "input.txt:2" → "hello" → FORWARD                  │
│   AckedTuple["input.txt:2"] = true                             │
│   Log: "PROCESSED,input.txt:2"                                 │
│   Ack → Leader                                                  │
│   Forward → Stage 2 Worker 1                                    │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Stage 2: Replace ("hello" → "hi")                               │
│                                                                 │
│ Worker 2-0: "input.txt:0" → "hello" → "hi"                     │
│   AckedTuple["input.txt:0"] = true                             │
│   Log: "PROCESSED,input.txt:0"                                 │
│   Ack → Worker 1-0                                              │
│   Result → Leader                                               │
│                                                                 │
│ Worker 2-1: "input.txt:2" → "hello" → "hi"                     │
│   AckedTuple["input.txt:2"] = true                             │
│   Log: "PROCESSED,input.txt:2"                                 │
│   Ack → Worker 1-2                                              │
│   Result → Leader                                               │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Leader: Final Output                                            │
│                                                                 │
│ WaitedTuples: {}  // All acked                                 │
│ DoneTuples: {                                                   │
│   "input.txt:0": true,                                          │
│   "input.txt:2": true                                           │
│ }                                                               │
│                                                                 │
│ Console Output:                                                 │
│   input.txt:0: hi                                               │
│   input.txt:2: hi                                               │
│                                                                 │
│ HyDFS Output File:                                              │
│   input.txt:0: hi                                               │
│   input.txt:2: hi                                               │
└─────────────────────────────────────────────────────────────────┘
```

## Ack Forwarding Mechanism

```
┌─────────────────────────────────────────────────────────────────┐
│ How Acks Are Forwarded                                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ When a worker receives an ack from downstream:                  │
│                                                                 │
│   HandleAck(id):                                                │
│     1. Mark tuple as acked: AckedTuple[id] = true              │
│     2. Log to HyDFS: "PROCESSED,<id>"                          │
│     3. Forward ack to ALL upstream workers:                    │
│        prev = GetAliveMembersInStage(Stage - 1)                │
│        for each upstream worker:                               │
│            sendAck(worker.Hostname, worker.RPCPort, id)        │
│                                                                 │
│ Note: This is simpler than storing ProcessedTuple because:     │
│   - We forward to ALL upstream workers (not just the sender)   │
│   - No need to track which worker sent which tuple             │
│   - Handles cases where upstream worker might have changed     │
│                                                                 │
│ Flow Example:                                                   │
│   ┌──────────┐      ┌──────────┐      ┌──────────┐            │
│   │ Stage 1  │─────>│ Stage 2  │─────>│ Stage 3  │            │
│   │ Worker   │      │ Worker   │      │ Worker   │            │
│   └──────────┘      └──────────┘      └──────────┘            │
│        │                 │                 │                    │
│        │                 │                 │                    │
│        │                 │ Ack             │                    │
│        │<────────────────│                 │                    │
│        │                 │                 │                    │
│        │                 │ Forward to ALL  │                    │
│        │                 │ Stage 1 workers │                    │
│        │                 │ (via membership)│                    │
│                                                                 │
│ Last Stage:                                                     │
│   ┌──────────┐      ┌──────────┐                              │
│   │ Stage 1  │─────>│ Stage 2  │                              │
│   │ Worker   │      │ Worker   │  (LAST STAGE)                │
│   └──────────┘      └──────────┘                              │
│        │                 │                                      │
│        │                 │                                      │
│        │                 │ Process & Ack                        │
│        │<────────────────│                                      │
│        │                 │                                      │
│        │                 │ Forward to ALL                       │
│        │                 │ Stage 1 workers                      │
│        │                 │                                      │
│        │                 │ Result                               │
│        │                 │───────────┐                          │
│        │                 │           ▼                          │
│        │                 │      Leader                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## State Recovery Example

```
┌─────────────────────────────────────────────────────────────────┐
│ Before Failure                                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ Worker 1-0:                                                     │
│   AckedTuple: {                                                 │
│     "file:0": true,                                             │
│     "file:1": true,                                             │
│     "file:2": true                                              │
│   }                                                             │
│                                                                 │
│ HyDFS Log: "Task-1-0.log"                                      │
│   PROCESSED,file:0                                              │
│   PROCESSED,file:1                                              │
│   PROCESSED,file:2                                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Process killed
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ After Restart                                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ New Worker 1-0 (same identity):                                │
│   1. RecoverStateFromHyDFS():                                  │
│      - Download "Task-1-0.log"                                 │
│      - Parse each line                                         │
│      - For "PROCESSED,file:0":                                 │
│          AckedTuple["file:0"] = true                           │
│      - For "PROCESSED,file:1":                                 │
│          AckedTuple["file:1"] = true                           │
│      - For "PROCESSED,file:2":                                 │
│          AckedTuple["file:2"] = true                           │
│                                                                 │
│   2. AckedTuple restored: {                                    │
│        "file:0": true,                                         │
│        "file:1": true,                                         │
│        "file:2": true                                          │
│      }                                                          │
│                                                                 │
│   3. If duplicate tuple arrives:                               │
│      - Check AckedTuple → already processed                    │
│      - Send ack (idempotent)                                   │
│      - Don't reprocess                                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```


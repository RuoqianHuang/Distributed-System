# Distributed Spark Streaming Setup Guide

## Overview

For MP4, we need to run Spark Streaming in **distributed cluster mode** across all 10 VMs. This requires:

1. **Spark cluster** running on all VMs
2. **Shared storage** (HDFS) so all workers can access input files

## Prerequisites

- Access to all 10 VMs: fa25-cs425-b601 through fa25-cs425-b610
- Java 8 or 11 installed on all VMs

## Step 1: Set Up HDFS (Shared Storage)

Since Spark workers need access to the same input files, we need shared storage. 

HDFS integrates well with Spark and is the standard approach.

#### Install HDFS (installed)

1. **Download Hadoop** (includes HDFS):
   ```bash
   cd /cs425/mp4
   wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
   tar -xzf hadoop-3.3.6.tar.gz
   mv hadoop-3.3.6 hadoop
   ```

2. **Configure HDFS** (on master VM):
   ```bash
   cd /cs425/mp4/hadoop/etc/hadoop
   
   # Edit core-site.xml
   cat > core-site.xml << EOF
   <configuration>
       <property>
           <name>fs.defaultFS</name>
           <value>hdfs://fa25-cs425-b601.cs.illinois.edu:9000</value>
       </property>
   </configuration>
   EOF
   
   # Edit hdfs-site.xml
   cat > hdfs-site.xml << EOF
   <configuration>
       <property>
           <name>dfs.replication</name>
           <value>3</value>
       </property>
       <property>
           <name>dfs.namenode.name.dir</name>
           <value>/cs425/mp4/hadoop-data/namenode</value>
       </property>
       <property>
           <name>dfs.datanode.data.dir</name>
           <value>/cs425/mp4/hadoop-data/datanode</value>
       </property>
   </configuration>
   EOF
   ```

3. **Format and Start HDFS**:
   ```bash
   # Format namenode (only once, on master)
   /cs425/mp4/hadoop/bin/hdfs namenode -format
   
   # Start namenode (on master)
   /cs425/mp4/hadoop/sbin/start-dfs.sh

## Step 2: Set Up Spark Cluster

### 2.1 Install Spark on All VMs (installed)

**On master VM** (fa25-cs425-b601):
```bash
./setup_spark.sh
source ~/.bashrc
```

**On each worker VM** (b602-b610):
```bash
# Deploy spark on all VMs:
./deploy_all.sh
```

### 2.2 Configure Spark for HDFS

**On master VM**, edit `$SPARK_HOME/conf/spark-defaults.conf`:
```bash
cat >> $SPARK_HOME/conf/spark-defaults.conf << EOF

# HDFS Configuration
spark.hadoop.fs.defaultFS hdfs://fa25-cs425-b601.cs.illinois.edu:9000

# Distributed mode settings
spark.executor.memory 2g
spark.executor.cores 2
spark.executor.instances 9
EOF
```

### 2.3 Start Spark Cluster

**On master VM**:
```bash
./configure_master.sh
./start_cluster.sh
```

**Verify cluster**:
- Master UI: `http://fa25-cs425-b601.cs.illinois.edu:8080`
- Should show 9 workers connected

## Resources

- [HDFS Quick Start](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)
- [Spark Cluster Mode](https://spark.apache.org/docs/latest/cluster-overview.html)
- [Spark on HDFS](https://spark.apache.org/docs/latest/rdd-programming-guide.html#external-datasets)


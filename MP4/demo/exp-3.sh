
#!/bin/bash

cd ..
make rainstorm
make worker
cd demo

WORKER_BIN_PATH=../bin/worker
RAINSTORM=../bin/rainstorm

Nstages=2
Ntasks=3

OP1=../rainstorm_ops/gplus_transform.py
ARG1="none"
T1="transform"

OP2=../rainstorm_ops/count_by_key.py
ARG2="none"
T2="aggregate"

N_SRC_FILE=1
SRC_FILE="dataset_gplus"

AUTOSCALE=false
INPUT_RATE=1000
LW=0
HW=2000

DEST_FILE="exp-3"
ONCE=true

${RAINSTORM} ${WORKER_BIN_PATH} ${Nstages} ${Ntasks} \
    ${OP1} ${ARG1} ${T1} \
    ${OP2} ${ARG2} ${T2} \
    ${N_SRC_FILE} ${SRC_FILE} \
    ${AUTOSCALE} ${INPUT_RATE} ${LW} ${HW} \
    ${DEST_FILE} ${ONCE}








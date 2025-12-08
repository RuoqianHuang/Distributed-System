
#!/bin/bash

cd ..
make rainstorm
make worker
cd demo


WORKER_BIN_PATH=../bin/worker
RAINSTORM=../bin/rainstorm

Nstages=2
Ntasks=3

OP1=../rainstorm_ops/filter.py
ARG1="Street"
T1="filter"

OP2=../rainstorm_ops/project.py
ARG2="none"
T2="transform"

N_SRC_FILE=1
SRC_FILE="dataset1"

AUTOSCALE=true
INPUT_RATE=100
LW=90
HW=110

DEST_FILE="application-2"
ONCE=true

${RAINSTORM} ${WORKER_BIN_PATH} ${Nstages} ${Ntasks} \
    ${OP1} ${ARG1} ${T1} \
    ${OP2} ${ARG2} ${T2} \
    ${N_SRC_FILE} ${SRC_FILE} \
    ${AUTOSCALE} ${INPUT_RATE} ${LW} ${HW} \
    ${DEST_FILE} ${ONCE}








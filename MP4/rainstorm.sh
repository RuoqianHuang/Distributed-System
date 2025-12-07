#!/bin/bash


make worker
make rainstorm
make monitor

WORKER_BIN_PATH=./bin/worker
Nstages=2
Ntasks=3

OP1=./rainstorm_ops/identity_key_is_line.py
ARG1="none"
T1="transform"

OP2=./rainstorm_ops/count_line.py
ARG2="none"
T2="aggregate"

N_SRC_FILE=1
SRC_FILE1="rainstorm_test"

AUTOSCALE=true
INPUT_RATE=200
LW=150
HW=250

DEST_FILE="rainres"
ONCE=false

./bin/rainstorm ${WORKER_BIN_PATH} ${Nstages} ${Ntasks} \
    ${OP1} ${ARG1} ${T1} \
    ${OP2} ${ARG2} ${T2} \
    ${N_SRC_FILE} ${SRC_FILE} \
    ${AUTOSCALE} ${INPUT_RATE} ${LW} ${HW} \
    ${DEST_FILE} ${ONCE}

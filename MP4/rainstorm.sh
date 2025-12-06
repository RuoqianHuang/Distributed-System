#!/bin/bash


make worker
make rainstorm
make monitor

WORKER_BIN_PATH=./bin/worker
Nstages=2
Ntasks=3

OP1=./rainstorm_ops/identity.py
ARG1="none"
T1="transform"

OP2=./rainstorm_ops/identity.py
ARG2="none"
T2="transform"

SRC_FILE="rainstorm_test"

AUTOSCALE=false
INPUT_RATE=100
LW=75
HW=120

DEST_FILE="rainstorm_result"
ONCE=true

./bin/rainstorm ${WORKER_BIN_PATH} ${Nstages} ${Ntasks} \
    ${OP1} ${ARG1} ${T1} \
    ${OP2} ${ARG2} ${T2} \
    ${SRC_FILE} \
    ${AUTOSCALE} ${INPUT_RATE} ${LW} ${HW} \
    ${DEST_FILE} ${ONCE}

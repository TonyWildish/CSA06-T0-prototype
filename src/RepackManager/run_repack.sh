#!/bin/bash

export STAGE_HOST=castorcms
#export STAGER_TRACE=1
. $T0ROOT/runtime.sh
t0Repacker $1

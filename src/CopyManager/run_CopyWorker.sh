#!/bin/bash

export T0_BASE_DIR=/afs/cern.ch/user/c/cmsprod/public/TransferTest

export T0ROOT=${T0_BASE_DIR}/T0

#
# Setup extra PERL environment
#

export PERL5LIB=${T0ROOT}/perl_lib:${T0_BASE_DIR}/ApMon_perl-2.2.6:${T0_BASE_DIR}/perl

perl $T0ROOT/src/CopyManager/CopyWorker.pl --config ${T0_BASE_DIR}/Config/TransferSystem_Cessy.cfg


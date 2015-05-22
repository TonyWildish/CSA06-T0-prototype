#!/bin/bash

export T0_BASE_DIR=/afs/cern.ch/user/p/pgoglov/public/T0

export CMSSW_BASE=$T0_BASE_DIR/CMSSW_CURRENT
export T0ROOT=$CMSSW_BASE/src/COMP/T0
export CONFIG=$T0ROOT/src/Config/IP5DevPrototype.conf

#
# Setup CMSSW runtime environment
#

. $T0ROOT/runtime.sh

#
# Setup extra PERL environment
#

export PERL5LIB=${T0ROOT}/perl_lib:${T0_BASE_DIR}/ApMon_perl-2.2.6:/afs/cern.ch/user/w/wildish/public/perl:/home/cmsmtcc/IP5DevT0


#
# Run perl script
#

perl -w /afs/cern.ch/user/p/pgoglov/public/T0_DEV/CMSSW_1_2_0/src/COMP/T0/src/DAQUpdator/DAQWatch.pl $@

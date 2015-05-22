#!/bin/bash
pushd /afs/cern.ch/user/c/cmsprod/prod/CSA06/T0
. ./env.sh
popd
perl $T0ROOT/src/PromptReconstruction/PromptReconstructionWorker.pl --config $T0ROOT/src/Config/CSA06.conf

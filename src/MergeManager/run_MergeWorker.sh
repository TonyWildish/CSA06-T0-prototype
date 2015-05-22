#!/bin/bash
pushd /afs/cern.ch/user/c/cmsprod/prod/CSA06/T0
. ./env.sh
. ./runtime_pr_106.sh
popd
perl $T0ROOT/src/MergeManager/MergeWorker.pl --config $T0ROOT/src/Config/CSA06106Merge.conf

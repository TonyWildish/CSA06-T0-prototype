#!/bin/bash
pushd /afs/cern.ch/user/w/wildish/public/T0/July
. ./env.sh
popd
perl $T0ROOT/src/RepackManager/RepackWorker.pl --config $TROOT/src/Config/JulyPrototype.conf

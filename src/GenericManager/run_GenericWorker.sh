#!/bin/bash
pushd /afs/cern.ch/user/w/wildish/public/T0/July
. ./env.sh
popd
perl $T0ROOT/src/GenericManager/GenericWorker.pl --config $T0ROOT/src/Config/DevPrototype.conf

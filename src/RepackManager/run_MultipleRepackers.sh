#!/bin/bash
pushd /afs/cern.ch/user/w/wildish/public/T0/July
. ./env.sh
popd

for i in 1 2 3 4
do
  perl $T0ROOT/src/Utilities/Damian.pl --logfile RepackWorker-$i.log --verbose \
	-- $T0ROOT/src/RepackManager/RepackWorker.pl \
	--config $T0ROOT/src/Config/JulyPrototype.conf
done

perl $T0ROOT/src/RepackManager/RepackWorker.pl --config $T0ROOT/src/Config/JulyPrototype.conf

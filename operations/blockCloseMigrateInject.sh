#!/usr/local/bin/bash

#
# This is an example, modify according to your needs !!!
#

. /data/cmsprod/T0PAProd/prodagent_init.sh

datasetpath="/GlobalNov07-EcalMIP/Online-CMSSW_1_7_1/RAW"
blockname="/GlobalNov07-EcalMIP/Online-CMSSW_1_7_1/RAW#3ef31664-b4be-4774-bd6e-5925bb9ca86c"

./blockCloseMigrateInject.py --datasetpath $datasetpath --blockname $blockname



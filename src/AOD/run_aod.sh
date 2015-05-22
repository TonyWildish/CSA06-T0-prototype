#!/bin/bash

export STAGE_HOST=castorcms
. $T0ROOT/runtime_pr.sh
export CMSSW_SEARCH_PATH=${CMSSW_SEARCH_PATH}:.
export PATH=/bin:/usr/bin:/usr/local/bin:$PATH

cfg=$1
PsetHash=`EdmConfigHash < $cfg`
Version=`echo $SCRAMRT_SET | awk -F_ '{ print $2"_"$3"_"$4 }'`
echo T0Signature: PsetHash=$PsetHash Version=$Version
cat $cfg | sed -e 's%^%T0InputCfgFile: %'

echo "`date` : Run aod for... $cfg"
cmsRun $cfg
status=$?
cat FrameworkJobReport.xml | sed -e 's%^%T0FrameworkJobReport: %'
cat PoolFileCatalog.xml    | sed -e 's%^%T0PoolFileCatalog: %'
cksum *.*.root | awk '{ print "T0Checksums: "$1" "$2" "$3 }'
echo "`date`: exiting. Status $status"
exit $status

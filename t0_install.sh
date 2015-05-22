#!/usr/local/bin/bash
#
#  script checks out CMSSW and adds all the
#  necessary packages to run the T0 prototype
#  (code checkout, code compilation etc.)
#

#
# CMSSW_BASE_DIR should be on AFS so that the worker batch jobs can access it
#
export CMSSW_BASE_DIR="to_be_modified"

if [ ! -d $CMSSW_BASE_DIR ]; then
    echo "CMSSW_BASE_DIR does not exist or is no directory"
    exit
fi

#
# CMSSW version
#  You most likely will have to change other
#  parts of the script if you change this
#
export CMSSW_VERSION=CMSSW_0_9_0

#
# CMSSW directory
#
export CMSSW_DIR=${CMSSW_BASE_DIR}/${CMSSW_VERSION}

if [ -d $CMSSW_DIR ]; then
    echo "CMSSW directory exists, aborting installation"
    exit;
fi

#
# Top directory of the T0 software
#
export T0ROOT=${CMSSW_DIR}/src/COMP/T0

cd $CMSSW_BASE_DIR
scramv1 project CMSSW ${CMSSW_VERSION}

cd $CMSSW_DIR/src

# rfio fixes
cvs co -r vi030806pm Utilities/RFIOAdaptor
cvs co -r vi030806pm Utilities/StorageFactory

# reconstruction fixes
cvs co RecoMuon/MuonSeedGenerator/data/MuonSeedProducer.cfi
cvs co Configuration/Examples/data/RECO081_noRS.cff
cvs co Configuration/Examples/data/RECO081_noRS.cfg
#ln -s Configuration/Examples/data/RECO081_noRS.cfg .
 
#cvs co Configuration/Examples/data/RECO081.cfg
#ln -s Configuration/Examples/data/RECO081.cfg .
 
# fast merge fixes
cvs checkout -r V01-06-03 IOPool/Common

mkdir T0
( cd T0 ; cvs co -d Repacker -r vi030806pm COMP/T0/Repacker )

scramv1 b

cd $CMSSW_DIR/src
cvs checkout COMP/T0

scramv1 runtime -csh > ${T0ROOT}/runtime.csh
scramv1 runtime -sh > ${T0ROOT}/runtime.sh
scramv1 runtime -sh > ${T0ROOT}/runtime_pr.sh


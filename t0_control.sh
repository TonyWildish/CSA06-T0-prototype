#!/bin/bash
#
# t0_control     starts and stops the T0 daemons and provides some cleanup between tests
#

#
# 
#
# T0_BASE_DIR is base directory for testing
# contains logs, depending on the config file
# can also contain Indices and PR logs
#
# Indices : ${T0_BASE_DIR}/Indices
# PR logs : ${T0_BASE_DIR}/Logs/pr
#
# If so configured, the cleanup will also
# delete Indices and PR logs
#
export T0_BASE_DIR="to_be_modified"

if [ ! -d $T0_BASE_DIR ]; then
    echo "T0_BASE_DIR does not exist or is no directory"
    exit
fi

#
# CMSSW directory
#
export CMSSW_DIR=/afs/cern.ch/user/h/hufnagel/public/T0Prototype/CMSSW_0_9_0

#
# Top directory of the T0 software
#
export T0ROOT=$CMSSW_DIR/src/COMP/T0

. /etc/init.d/functions

#
# Setup CMSSW runtime environment
#

. $T0ROOT/runtime.sh

#
# Setup extra PERL environment
#

export PERL5LIB=${T0ROOT}/perl_lib:${T0_BASE_DIR}/ApMon_perl-2.2.6:/afs/cern.ch/user/w/wildish/public/perl

RETVAL=0
PID=""

#
# Define rules to start and stop daemons
#
start(){
    mkdir -p ${T0_BASE_DIR}/Logs/Logger
    mkdir -p ${T0_BASE_DIR}/Logs/FakeFileGenerator
    mkdir -p ${T0_BASE_DIR}/Logs/StorageManager
    mkdir -p ${T0_BASE_DIR}/Logs/RepackManager
    mkdir -p ${T0_BASE_DIR}/Logs/ExportManager
    mkdir -p ${T0_BASE_DIR}/Logs/PromptReconstruction
    mkdir -p ${T0_BASE_DIR}/Logs/Utilities
    mkdir -p ${T0_BASE_DIR}/Logs/pr

    mkdir -p ${T0_BASE_DIR}/workdir
    cd ${T0_BASE_DIR}/workdir

    prog="Logger/LoggerReceiver"
    echo -n $"Starting $prog "
    ${T0ROOT}/src/${prog}.pl --config ${T0_BASE_DIR}/Config/JulyPrototype.conf > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
    sleep 2
    echo

    prog="FakeFileGenerator/OnTheFlyFakeIndexGenerator"
    echo -n $"Starting $prog "
    ${T0ROOT}/src/${prog}.pl --config ${T0_BASE_DIR}/Config/JulyPrototype.conf > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
    sleep 2
    echo

    prog="StorageManager/StorageManager"
    echo -n $"Starting $prog "
    ${T0ROOT}/src/${prog}.pl --config ${T0_BASE_DIR}/Config/JulyPrototype.conf > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
    sleep 2
    echo

    prog="RepackManager/RepackManager"
    echo -n $"Starting $prog "
    ${T0ROOT}/src/${prog}.pl --config ${T0_BASE_DIR}/Config/JulyPrototype.conf > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
    sleep 2
    echo

    prog="PromptReconstruction/PromptReconstructionManager"
    echo -n $"Starting $prog "
    ${T0ROOT}/src/${prog}.pl --config ${T0_BASE_DIR}/Config/JulyPrototype.conf > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
    sleep 2
    echo

#    prog="ExportManager/ExportManager"
#    echo -n $"Starting $prog "
#    ${T0ROOT}/src/${prog}.pl --config ${T0_BASE_DIR}/Config/JulyPrototype.conf > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
#    sleep 2
#    echo

#    prog="StorageManager/StorageManagerWorker"
#    echo $"Submitting $prog "
#    ${T0ROOT}/src/${prog}.pl --config ${T0_BASE_DIR}/Config/JulyPrototype.conf > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
#    for i in `seq 1 20`;
#    do
#	bsub -q dedicated -R itdccms -g /t0prototype/storagemanager/worker ${T0ROOT}/src/StorageManager/run_StorageManagerWorker.sh
#	sleep 2
#    done    

#    prog="RepackManager/RepackWorker"
#    echo $"Submitting $prog "
#    ${T0ROOT}/src/${prog}.pl --config ${T0_BASE_DIR}/Config/JulyPrototype.conf > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
#    for i in `seq 1 60`;
#    do
#	bsub -q dedicated -R itdccms -g /t0prototype/repackmanager/worker ${T0ROOT}/src/RepackManager/run_RepackWorker.sh
#	bsub -q dedicated -R itdccms -g /t0prototype/repackmanager/worker ${T0ROOT}/src/RepackManager/run_MultipleRepackers.sh
#	sleep 2
#    done

#    prog="PromptReconstruction/PromptReconstructionWorker"
#    echo $"Submitting $prog "
#    ${T0ROOT}/src/${prog}.pl --config ${T0_BASE_DIR}/Config/JulyPrototype.conf > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
#    for i in `seq 1 120`;
#    do
#	bsub -q dedicated -R itdccms -g /t0prototype/promptreconstruction/worker ${T0ROOT}/src/PromptReconstruction/run_PromptReconstructionWorker.sh
#	sleep 2
#    done

#    prog="Utilities/FileFeeder"
#    echo $"Submitting $prog "
#    ${T0ROOT}/src/${prog}.pl --directory lxcmsa:/data2/wildish/T0/PRInput --notify ExportReady --interval 10 > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
#    ${T0ROOT}/src/${prog}.pl --directory /castor/cern.ch/cms/store/unmerged/RelVal/2006/7/24 --notify ExportReady --interval 10 > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
#    ${T0ROOT}/src/${prog}.pl --directory cmslcgse02:/data1/hufnagel/T0/PRInput --notify ExportReady --interval 10 > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
#    sleep 2
#    echo

}

stop(){
    bkill -g /t0prototype/repackmanager/worker 0
    bkill -g /t0prototype/storagemanager/worker 0
    bkill -g /t0prototype/promptreconstruction/worker 0
    prog="/usr/bin/perl"
    echo -n $"Stopping $prog "
#    PID=`/sbin/pidof $prog`
#    kill $PID
    killall $prog
    echo
}

status(){
    prog="/usr/bin/perl"
    echo -n $"Checking $prog : "
    PID=`/sbin/pidof $prog`
    echo $PID
    echo
}

cleanup(){
    find ${T0_BASE_DIR}/Logs -type f -name "*.log*" -exec rm -f {} \;

    find ${T0_BASE_DIR}/Logs -type f -name "*.out.gz*" -exec rm -f {} \;

    if [ -d ${T0_BASE_DIR}/Indices ]; then
      find ${T0_BASE_DIR}/Indices -type f -name "*.idx" -exec rm -f {} \;
    fi
}


# See how we were called.
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
        status
        ;;
    cleanup)
        cleanup
        ;;
    *)
        echo $"Usage: $0 {start|stop|status|cleanup}"
        RETVAL=1
esac
                                                                                                                                                                            
exit $RETVAL

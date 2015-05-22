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
export T0_BASE_DIR="/home/cmsprod/TransferTest"

if [ ! -d $T0_BASE_DIR ]; then
    echo "T0_BASE_DIR does not exist or is no directory"
    exit
fi

#
# Top directory of the T0 software
#
export T0ROOT=/home/cmsprod/TransferTest/T0

. /etc/init.d/functions

#
# Configuration file
#
export T0_CONFIG=${T0_BASE_DIR}/Config/TransferSystem_Cessy.cfg

#
# Local run directory
#
export T0_LOCAL_RUN_DIR=${T0_BASE_DIR}

# Setup extra PERL environment
#
export PERL5LIB=${T0ROOT}/perl_lib:${T0_BASE_DIR}/ApMon_perl-2.2.6:${T0_BASE_DIR}/perl

RETVAL=0
PID=""

#
# Setup Oracle environment
#

#export ORACLE_HOME="/usr/lib/oracle/10.2.0.1"
#export TNS_ADMIN="/afs/cern.ch/project/oracle/admin"
#export LD_LIBRARY_PATH="/usr/lib/oracle/10.2.0.1/client/lib"
#export NLS_LANG=''

#
# Setup CASTOR environment
#
export STAGE_HOST=castorcms
export RFIO_USE_CASTOR_V2=YES
export STAGE_SVCCLASS=t0input

#
# Define rules to start and stop daemons
#
start(){
    #
    # Setting up environment
    #
    mkdir -p ${T0_LOCAL_RUN_DIR}/Logs/CopyManager

    mkdir -p ${T0_BASE_DIR}/workdir
    cd ${T0_BASE_DIR}/workdir

    prog="CopyManager/CopyWorker"
    echo -n $"Starting $prog"

    nohup ${T0ROOT}/src/${prog}.pl --config $T0_CONFIG >> ${T0_LOCAL_RUN_DIR}/Logs/${prog}.log 2>&1 &
    sleep 10
    echo
}

stop(){
    pidlist=""
    for pid in $(/bin/ps ax -o pid,command | grep '/usr/bin/perl -w' | grep ${T0ROOT}/src/ | grep $T0_CONFIG | sed 's/^[ \t]*//' | cut -d' ' -f 1 )
    do
      pidlist="$pidlist $pid"
    done
    if [ -n "$pidlist" ] ; then
      kill $pidlist
    fi
}

status(){
    for pid in $(/bin/ps ax -o pid,command | grep '/usr/bin/perl -w' | grep ${T0ROOT}/src/ | grep $T0_CONFIG | sed 's/^[ \t]*//' | cut -d' ' -f 1 )
    do
      echo `/bin/ps $pid | grep $pid`
    done
#    prog="/usr/bin/perl"
#    echo -n $"Checking $prog : "
#    PID=`/sbin/pidof $prog`
#    echo $PID
#    echo
}

cleanup(){
    find ${T0_LOCAL_RUN_DIR}/Logs -type f -name "*.log*" -exec rm -f {} \;
    find ${T0_LOCAL_RUN_DIR}/Logs -type f -name "*.out.gz*" -exec rm -f {} \;
    find ${T0_LOCAL_RUN_DIR}/workdir/ -type f -name "*.log" -exec rm -f {} \;
}


# See how we were called.
case "$1" in
    start)
        if [ ! -z $2 ];
	  then export T0_CONFIG=$2
	fi
	
	echo "Using config file : $T0_CONFIG" 
 
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
        echo $"Usage: $0 {start|stop|status|cleanup} [config_file]"
        RETVAL=1
esac
                                                                                                                                                                            
#exit $RETVAL

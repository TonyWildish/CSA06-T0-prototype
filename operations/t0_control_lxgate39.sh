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
export T0_BASE_DIR=/data/cmsprod/TransferTest

if [ ! -d $T0_BASE_DIR ]; then
    echo "T0_BASE_DIR does not exist or is no directory"
    exit
fi

#
# Top directory of the T0 software
#
export T0ROOT=/afs/cern.ch/user/c/cmsprod/public/TransferTest/T0

. /etc/init.d/functions

#
# Configuration file
#
export T0_CONFIG=${T0_BASE_DIR}/Config/TransferSystem_CERN.cfg

#
# Setup extra PERL environment
#

export PERL5LIB=${T0ROOT}/perl_lib:${T0_BASE_DIR}/ApMon_perl-2.2.6:/afs/cern.ch/user/c/cmsprod/public/TransferTest/perl

#
# Setup ProdAgent environment
#

. /data/cmsprod/T0PAProd/prodagent_init.sh

export STAGE_SVCCLASS=t0export

export X509_HOST_CERT=/data/cmsprod/T0PAProd/certs/tier0cert.pem
export X509_HOST_KEY=/data/cmsprod/T0PAProd/certs/tier0key.pem


RETVAL=0
PID=""

#
# Define rules to start and stop daemons
#
start(){
    mkdir -p ${T0_BASE_DIR}/Logs/Logger
    mkdir -p ${T0_BASE_DIR}/Logs/CopyCheckManager
    mkdir -p ${T0_BASE_DIR}/Logs/DBSUpdator

    mkdir -p ${T0_BASE_DIR}/workdir
    cd ${T0_BASE_DIR}/workdir

    prog="Logger/LoggerReceiver"
    echo -n $"Starting $prog "
    ${T0ROOT}/src/${prog}.pl --config $T0_CONFIG > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
    sleep 2
    echo

    prog="CopyCheckManager/CopyCheckManager"
    echo -n $"Starting $prog "
    ${T0ROOT}/src/${prog}.pl --config $T0_CONFIG > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
    sleep 2
    echo

    prog="CopyCheckManager/CopyCheckWorker"
    echo -n $"Starting $prog "
    ${T0ROOT}/src/${prog}.pl --config $T0_CONFIG > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
    sleep 2
    echo

    prog="DBSUpdator/DBSUpdator"
    echo -n $"Starting $prog "
    ${T0ROOT}/src/${prog}.pl --config $T0_CONFIG > ${T0_BASE_DIR}/Logs/${prog}.log 2>&1 &
    sleep 2
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
    find ${T0_BASE_DIR}/Logs -type f -name "*.log*" -exec rm -f {} \;
    find ${T0_BASE_DIR}/Logs -type f -name "*.out.gz*" -exec rm -f {} \;
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

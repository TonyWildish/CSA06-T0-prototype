#!/bin/bash
                                                                                                                                    
#
# Script only needs to set the PERL5LIB environment
# and then calls a perl script to inject a file into
# the transfer system
#
# It is recommended to set PER5LIB within your shell
# and call the perl script directly.
#
                                                                                                                                    
export T0_BASE_DIR=/nfshome0/cmsprod/TransferTest
export T0ROOT=${T0_BASE_DIR}/T0
export CONFIG=${T0_BASE_DIR}/Config/TransferSystem_Cessy.cfg
 
export PERL5LIB=${T0ROOT}/perl_lib:${T0_BASE_DIR}/perl
 
${T0_BASE_DIR}/injection/sendNotification.pl --config $CONFIG $@


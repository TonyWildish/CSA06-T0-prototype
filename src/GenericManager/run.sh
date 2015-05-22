. /afs/cern.ch/user/w/wildish/public/COMP/env.sh 
./GenericManager --config prestage.conf &
../LoggerReceiver.pl --config prestage.conf &
./GenericWorker --config prestage.conf &
cat /afs/cern.ch/user/w/wildish/public/COMP/PHEDEX_CVS/sorted-pfns.txt | ./LoggerSender.pl --config prestage.conf

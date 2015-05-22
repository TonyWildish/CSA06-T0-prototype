bsub -q cmscsa06 -R 'type=SLC4' -o ~/w5/CSA06/logs/AODWorker.%J.log -g /CSA/105AOD run_AOD.sh

bsub -q cmscsa06 -R 'type=SLC4' -o ~/w5/CSA06/logs/MergeWorker.%J.log -g /CSA/Merge run_MergeWorker.sh

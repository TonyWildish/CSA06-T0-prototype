bsub -q cmscsa06 -R 'type=SLC4' -o ~/w5/CSA06/logs/ARWorker.%J.log -g /CSA/AR run_AlcaReco.sh

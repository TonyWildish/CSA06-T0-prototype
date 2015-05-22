#!/bin/bash                                                                     

# 1: pfn or lfn
# 2: size (optional, but recommended)
# 3. cksum (optional
# 4. dataset
# 5. block
echo "$0: $1 $2 $3 $4 $5"

/data/rehn/Tier0Export/SITECONF/CERN/Tier0/DropGenerator \
 -input $1,$2,$3	\
 -dataset $4		\
 -block $5		\
 -output /data/rehn/Tier0Export/Prod_CERN/Tier0/state/drop-publish/inbox\
 -dbsname 'http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquery?instance=MCGlobal/Writer' \
 -dlsname 'lfc://prod-lfc-cms-central.cern.ch/grid/cms/DLS/LFC'
status=$?
sleep 1

echo "`date` : status=$status for $1 $2 $3 $4 $5" | tee -a /data/CSA06/logs/make_drops.log

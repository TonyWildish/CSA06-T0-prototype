#!/usr/bin/env python

"""
_closeMigrateInject_

for provided datasetpath close any open blocks, migrate all
blocks to global and inject them all into PhEDEx

"""

import sys
import getopt
import random
import time
import os

from MessageService.MessageService import MessageService

from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSReader
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError,DBSReaderError,formatEx
import ProdCommon.DataMgmt.DBS.DBSWriterObjects as DBSWriterObjects

from ProdCommon.DataMgmt.BlockTools import BlockManager

valid = [ 'datasetpath=' ]

usage = \
"""
Usage: closeMigrateInject.py



"""


try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)


datasetpath = None

for opt, arg in opts:
    if opt == "--datasetpath":
        datasetpath = arg

if datasetpath == None:
    msg = "--datasetpath option not provided"
    raise RuntimeError, msg

localDbsUrl = "https://cmst0dbs.cern.ch:8443/cms_dbs_prod_tier0_writer/servlet/DBSServlet"
globalDbsUrl = "https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet"

phedexConfig = "/data/cmsprod/T0PAProd/phedex/DBParam:Prod/TIER0"
phedexNodes = "T0_CH_CERN_Export"

dbsreader = DBSReader(localDbsUrl,level='ERROR')

#blocks = dbsreader.listOpenFileBlocks(datasetpath)
blocks = dbsreader.listFileBlocks(datasetpath)

for blockName in blocks:
     blockMgr = BlockManager(blockName, localDbsUrl, globalDbsUrl, datasetpath)
     blockMgr.closeBlock()
     blockMgr.migrateToGlobalDBS()
     blockMgr.injectBlockToPhEDEx(phedexConfig, phedexNodes)


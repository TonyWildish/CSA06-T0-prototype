#!/usr/bin/env python

"""
_blockCloseMigrateInject_

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

valid = [ 'datasetpath=', 'blockname=' ]

usage = \
"""
Usage: blockCloseMigrateInject.py



"""


try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)


datasetpath = None
blockname = None

for opt, arg in opts:
    if opt == "--datasetpath":
        datasetpath = arg
    if opt == "--blockname":
        blockname = arg

if datasetpath == None:
    msg = "--datasetpath option not provided"
    raise RuntimeError, msg
if blockname == None:
    msg = "--blockname option not provided"
    raise RuntimeError, msg

localDbsUrl = "https://cmst0dbs.cern.ch:8443/cms_dbs_prod_tier0_writer/servlet/DBSServlet"
globalDbsUrl = "https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet"

phedexConfig = "/data/cmsprod/TransferTest/phedex/DBParam:Prod/TIER0"
phedexNodes = "T0_CH_CERN_Export"

dbsreader = DBSReader(localDbsUrl,level='ERROR')

blockMgr = BlockManager(blockname, localDbsUrl, globalDbsUrl, datasetpath)
blockMgr.closeBlock()
blockMgr.migrateToGlobalDBS()
blockMgr.injectBlockToPhEDEx(phedexConfig, phedexNodes)


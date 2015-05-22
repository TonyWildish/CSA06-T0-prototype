#!/usr/bin/env python

"""
_triggerConversion_

trigger old-style conversion in old PA instance
with old RepackerInjector component

"""

import sys
import getopt
import random
import time
import os

from FwkJobRep.FileInfo import FileInfo
from FwkJobRep.FwkJobReport import FwkJobReport
from MessageService.MessageService import MessageService

from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSReader
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError,DBSReaderError,formatEx
import ProdCommon.DataMgmt.DBS.DBSWriterObjects as DBSWriterObjects

from ProdCommon.DataMgmt.BlockTools import BlockManager

valid = [ 'datasetpath=' ]

usage = \
"""
Usage: triggerConversion.py



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
phedexNodes= "T0_CH_CERN_Export"

primaryDataset = datasetpath.split('/')[1]
processedDataset = datasetpath.split('/')[2]

ms = MessageService()
ms.registerAs("Test")
ms.publish("RepackerInjector:StartNewRun","%d %s %s" % (0,primaryDataset,processedDataset))
ms.commit()


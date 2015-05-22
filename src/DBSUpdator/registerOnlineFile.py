#!/usr/bin/env python

"""
_registerOnlineFile_

Script to register online files in DBS

"""

import sys
import getopt
import random
import time
import os

from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport

from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError,formatEx,DBSReaderError
import ProdCommon.DataMgmt.DBS.DBSWriterObjects as DBSWriterObjects

from ProdCommon.DataMgmt.BlockTools import BlockManager

valid = [ 'run=', 'lumi=', 'lfn=', 'size=', 'cksum=', 'nevents=', 'primds=', 'procds=', 'type=', 'appname=', 'appversion=' ] 

usage = \
"""
Usage: registerOnlineFile.py



"""


try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

runNumber = None
lumiSection = None
lfn = None
fileSize = None
checkSum = None
nEvents = None
primaryDataset = None
processedDataset = None
dataTier = 'RAW'
fileType = None
appName = None
appVersion = None

for opt, arg in opts:
    if opt == "--run":
        runNumber = int(arg)
    if opt == "--lumi":
        lumiSection = int(arg)
    if opt == "--lfn":
        lfn = arg
    if opt == "--size":
        fileSize = int(arg)
    if opt == "--cksum":
        checkSum = arg
    if opt == "--nevents":
        nEvents = int(arg)
    if opt == "--primds":
        primaryDataset = arg
    if opt == "--procds":
        processedDataset = arg
    if opt == "--type":
        fileType = arg
    if opt == "--appname":
        appName = arg
    if opt == "--appversion":
        appVersion = arg


if runNumber == None:
    msg = "--run option not provided"
    raise RuntimeError, msg
if lfn == None:
    msg = "--lfn option not provided"
    raise RuntimeError, msg
if fileSize == None:
    msg = "--size option not provided"
    raise RuntimeError, msg
if checkSum == None:
    msg = "--cksum option not provided"
    raise RuntimeError, msg
if primaryDataset == None:
    msg = "--primds option not provided"
    raise RuntimeError, msg
if processedDataset == None:
    msg = "--procds option not provided"
    raise RuntimeError, msg
if fileType == None:
    msg = "--type option not provided"
    raise RuntimeError, msg
if appName == None:
    msg = "--appname option not provided"
    raise RuntimeError, msg
if appVersion == None:
    msg = "--appversion option not provided"
    raise RuntimeError, msg

#
# type dependent checks
#
if fileType == 'streamer' or fileType == 'edm' or fileType == 'pixdmp':
    if nEvents == None:
        msg = "--nevents option not provided"
        raise RuntimeError, msg
elif fileType == 'lumi':
    nEvents = 0
elif fileType == 'lumi-sa':
    nEvents = 0
elif fileType == 'lumi-vdm':
    nEvents = 0

# temporary, PRODCOMMON code does not support DBS registration w/0 lumi section anymore
if fileType  == 'pixdmp':
    lumiSection = 1
    
jobReport = FwkJobReport()
jobReportFile = jobReport.newFile()

if fileType == 'streamer':
    jobReportFile['FileType'] = 'STREAMER'
elif fileType == 'edm':
    jobReportFile['FileType'] = 'EDM'
elif fileType == 'lumi':
    jobReportFile['FileType'] = 'LUMI'
elif fileType == 'lumi-sa':
    jobReportFile['FileType'] = 'LUMI-SA'
elif fileType == 'lumi-vdm':
    jobReportFile['FileType'] = 'LUMI-VDM'
elif fileType == 'pixdmp':
    jobReportFile['FileType'] = 'PIXDMP'
else:
    msg = "files of type %s are not supported" % fileType
    raise RuntimeError, msg

##print "Generating Fake JobReportRun for %s file" % fileType
##print " for run : %s" % runNumber
##if ( lumiSection != None ):
##    print " for lumi section : %s" % lumiSection
##print " and dataset : /%s/%s/%s" % (primaryDataset, processedDataset, dataTier)

jobReportFile['LFN'] = lfn
jobReportFile['Size'] = fileSize
jobReportFile.addRunAndLumi(runNumber, )
jobReportFile.addChecksum('cksum',checkSum)

if ( lumiSection != None ):
    jobReportFile.addRunAndLumi(long(runNumber), long(lumiSection))
else:
    jobReportFile.addRunAndLumi(long(runNumber), [])

datasetStrmr = jobReportFile.newDataset()
datasetStrmr['PrimaryDataset'] = primaryDataset
datasetStrmr['PrimaryDatasetType'] = 'data'
datasetStrmr['ProcessedDataset'] = processedDataset
datasetStrmr['DataTier'] = dataTier

jobReportFile['TotalEvents'] = nEvents
jobReportFile['SEName'] = "srm.cern.ch"

##jobReport.write('FrameworkJobReport.xml')

localDbsUrl = "https://cmst0dbs.cern.ch:8443/cms_dbs_prod_tier0_writer/servlet/DBSServlet"

dbswriter = DBSWriter(localDbsUrl,level='ERROR')

primary = DBSWriterObjects.createPrimaryDataset(datasetStrmr, dbswriter.dbs)

datasetStrmr['ApplicationName'] = appName
datasetStrmr['ApplicationVersion'] = appVersion
datasetStrmr['ApplicationFamily'] = 'DAQ'
datasetStrmr['PSetHash'] = 'NA'
datasetStrmr['PSetContent'] = 'NA'

algo = DBSWriterObjects.createAlgorithm(datasetStrmr, None, dbswriter.dbs)

processed = DBSWriterObjects.createProcessedDataset(primary, algo, datasetStrmr, dbswriter.dbs)

try:
    blocks = dbswriter.insertFiles(jobReport, insertDetectorData = True)
except DBSWriterError, ex:
    print "%s"%ex

# limit block size to 500
for blockName in blocks:
    dbswriter.manageFileBlock(blockName, maxFiles=500)


#!/usr/bin/env python
"""
_checkDBSBlockConsistency_

"""

__revision__ = "$Id: checkDBSBlockConsitency.py,v 1.7 2009/04/08 18:57:51 sryu Exp $"
__version__ = "$Revision: 1.7 $"

import sys
import logging
import threading

from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *

from WMCore.WMBS.File import File as WMBSFile
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from T0.GenericTier0 import Tier0DB
from T0.DataStructs.T0ASTFile import T0ASTFile
from T0.State.Database.Reader import ListBlock
from T0.State.Database.Reader import ListFiles
from T0.State.Database.Reader import ListDatasets

class BlcokConsitencyTest(object):
    
    def __init__(self):
        paConfig = loadProdAgentConfiguration()    
        t0astDBConfig = paConfig.getConfig("Tier0DB")
    
        self.t0astDBConn = Tier0DB.Tier0DB(t0astDBConfig, manageGlobal = True)
        self.t0astDBConn.connect()
        
        localDBSConfig = paConfig.getConfig("LocalDBS")
        globalDBSConfig = paConfig.getConfig("GlobalDBSDLS")
        # phedexConfig = paConfig.getConfig("PhEDExConfig")
        
        self.localDBSUrl = localDBSConfig["DBSURL"]
        self.globalDBSUrl = globalDBSConfig["DBSURL"]
        
        # self.phedexDSUrl = phedexConfig["DataServiceURL"]
        self.localDbsApi = DbsApi({'url':localDBSConfig["DBSURL"]}) 
        self.globalDbsApi = DbsApi({'url':globalDBSConfig["DBSURL"]})
        
    def listMissingBlocksFromDBS(self, dbsApi, blockStatus, migrateStatus):
        blockList = ListBlock.listBlocksByStatus(self.t0astDBConn, blockStatus)
        missingBlockList = []
        for block in blockList:
            dbsBlock = dbsApi.listBlocks(block_name=block["BLOCK_NAME"])
            if dbsBlock == []:
                missingBlockList.append(block)
        return missingBlockList
    
    def listAllBlocksFromDBS(self, dbsApi, blockStatus, migrateStatus):
        blockList = ListBlock.listBlocksByStatus(self.t0astDBConn, blockStatus, migrateStatus)
        dbsBlockList = []
        for block in blockList:
            dbsBlock = dbsApi.listBlocks(block_name=block["BLOCK_NAME"])
            if dbsBlock != []:
                dbsBlockList.append(block)
        return dbsBlockList
    
    def printMissingBlockName(self, location, status, migrateStatus = None, verbose = 1):
        
        if location == "local":
            dbsApi = self.localDbsApi
            dbsUrl = self.localDBSUrl
        elif location == "global":
            dbsApi = self.globalDbsApi
            dbsUrl = self.globalDBSUrl
        
        verboseLevel = int(verbose)
        print "For the all %s, %s block -" % (status, migrateStatus)
        
        if verboseLevel > 2:
            allBlockList = self.listAllBlocksFromDBS(dbsApi, status, migrateStatus)
            print "All t0ast blocks in DBS : %s  number: %s" % (dbsUrl, len(allBlockList))
            self.printBlocksAndFiles(location, allBlockList, "all", verboseLevel)
                
        blockList = self.listMissingBlocksFromDBS(dbsApi, status, migrateStatus)
        if blockList == []:
            print "There is no missing blocks in DBS : %s" % dbsUrl 
        else:
            print "List of missing blocks from DBS : %s number: %s" % (dbsUrl, len(blockList))
            self.printBlocksAndFiles(location, blockList, "missing", verboseLevel)
            print "the number of missing block: %s" % len(blockList)
    
    def printBlocksAndFiles(self, location, blockList,  type = "missing",  verbose = 1):
        """
        print blocks and files: verbose = 1 only blocks, verbose > 1 blocks and files 
        """
        for block in blockList:
            print "\n"
            print "#################################################################"
            print "Block ID: %s Name: %s" % (block["BLOCK_ID"], block["BLOCK_NAME"])
            
            if verbose < 2:
                continue
            
            if location == "global":
                print "\n"
                print "List all files from %s block from Global DBS: Get info from local DBS " % type
                for file in self.localDbsApi.listFiles(blockName=block["BLOCK_NAME"]):
                    print "parent list: %s" % file["ParentList"]
                    print "file LFN: %s" % file["LogicalFileName"] 
                     
                print "\n"
                fileIDs = ListFiles.listFileIDsByBlockID(self.t0astDBConn,  block["BLOCK_ID"])
                print "List all files from %s block from Global DBS: Get info from T0AST " % type
                print "====================================================="
                for fileID in fileIDs:
                    wmbsFile = WMBSFile(id = fileID)
                    wmbsFile.load()
                    print "--------------------------------------"
                    print "Info from: T0AST"
                    print "file LFN: %s" % wmbsFile["lfn"]
                    print "" 
                    
                    file = T0ASTFile(wmbsFile)        
                    file.datasetPathID = \
                        ListDatasets.listDatasetIDForWMBSFile(self.t0astDBConn, wmbsFile["id"])
    
                    datasetNames = \
                        ListDatasets.listDatasetNamesForWMBSFile(self.t0astDBConn, wmbsFile["id"])
                    file["PRIMARY_DATASET"] = datasetNames["PRIMARY"]
                    file["PROCESSED_DATASET"] = datasetNames["PROCESSED"]
                    file["DATA_TIER"] = datasetNames["TIER"]
         
                    if file["DATA_TIER"] == "RECO":
                        t0ParentFileList =  file.getParentList(type="file")
                        
                        for wmbsFile in t0ParentFileList:
                            t0File = T0ASTFile(wmbsFile)
                            t0File["BLOCK_ID"] = ListBlock.getBlockIDByFileID(self.t0astDBConn, wmbsFile["id"])
                            
                            print "Block ID: %s : Parent File: %s" % (t0File["BLOCK_ID"], t0File["LFN"]) 
                            if t0File["BLOCK_ID"] != None:
                                blockInfo = ListBlock.getBlockInfoByID(self.t0astDBConn, t0File["BLOCK_ID"])
                                print "Block Name: %s \nStatus: %s" % (blockInfo["BLOCK_NAME"], blockInfo["STATUS"])
                                
                                if blockInfo["STATUS"] == "InFlight" or blockInfo["MIGRATE_STATUS"] == "Migrated":
                                    dbsBlock = self.localDbsApi.listBlocks(block_name=blockInfo["BLOCK_NAME"])
                                    if dbsBlock == []:
                                        print "It doesn't exist in Local dbs: Something wrong"
                                    else:
                                        print "Block: %s exist in Local DBS" % blockInfo["BLOCK_NAME"]
                                    
                                    try:    
                                        for file in self.localDbsApi.listFiles(patternLFN=t0File["LFN"]):
                                            print "File: %s exist in Local DBS" % file["LogicalFileName"]
                                    except:
                                        print "File doesn't exist in Local DBS"
                                        
                                if blockInfo["MIGRATE_STATUS"] == "Migrated":
                                    dbsBlock = self.globalDbsApi.listBlocks(block_name=blockInfo["BLOCK_NAME"])
                                    if dbsBlock == []:
                                        print "It doesn't exist in Global dbs: Something wrong"
                                    else:
                                        print "Block: %s exist in Global DBS" % blockInfo["BLOCK_NAME"]
                                        
                                    try:    
                                        for file in self.globalDbsApi.listFiles(patternLFN=t0File["LFN"]):
                                            print "File: %s exist in Global DBS" % file["LogicalFileName"]
                                    except:
                                        print "File doesn't exist in Global DBS"
                                
                        print "\n"
                        print "Info from Local DBS: List all parent files from %s block:" % type
                        
                        try:
                            for pfile in self.localDbsApi.listFileParents(file["LFN"]):
                                print "Parent Block: %s" % pfile["Block"]["NAME"]
                                print "Parent File: %s" % pfile["LogicalFileName"]
                                
                                print "Info from Global DBS: parent block for %s block:" % type
                                blockList = self.globalDbsApi.listBlocks(block_name=block["BLOCK_NAME"])
                                if blockList == []:
                                    print "Global DBS doen't have block %s "% pfile["Block"]["NAME"]
                                else:
                                    for dbsBlock in blockList:
                                        print "Global DBS Parent block %s exsist" % dbsBlock["NAME"]
                                    
                        except Exception, ex:
                            print "No parents file found in Local DBS "
                        
                        print "=====================================================" 
                        
    def __delete__(self):
        # probably not the way to close connection
        # for now use destructor
        self.t0astDBConn.close()
         
if __name__ == "__main__":

    if len(sys.argv) != 4:
        print "Usage:"
        print "  ./checkDBSBlockConsistency.py [block_status] [migrate_status] [verbose_level]"
        print """  block_status can be "InFlight" or "Exported" (Case sensitive)"""
        print """  block_status can be "NotMigrated" or "Migrated" (Case sensitive) and verbose_level can be 1 or 2 or 3"""
        
        sys.exit(1)
            
    blockTest = BlcokConsitencyTest()
    print "Compare t0ast blocks to Local DBS blocks" 
    blockTest.printMissingBlockName("local", sys.argv[1], sys.argv[2], sys.argv[3])
    
    print "\n\nCompare t0ast blocks to Global DBS blocks" 
    blockTest.printMissingBlockName("global", sys.argv[1], sys.argv[2], sys.argv[3])
    
    sys.exit(0)

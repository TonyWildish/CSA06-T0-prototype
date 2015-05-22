#!/usr/bin/env python
"""

Class which provide method for table initialization for RepackerComponents for Testing
"""

__revision__ = "$Id: RepackerTableSetup.py,v 1.18 2008/07/25 20:25:39 sryu Exp $"
__version__ = "$Revision: 1.18 $"

import os
import logging
import time
import random

from ProdCommon.Database.Connection import Connection
from ProdAgentDB.Config import loadConfig
import ProdAgentCore.LoggingUtils as LoggingUtils

from T0.RunConfigCache.FileBackend import FileBackend
from T0.RunConfigCache.RunConfig import RunConfig

dbConfig = loadConfig("Tier0DB")

def getTier0DBConfig():
    return loadConfig("Tier0DB")

def connectionHandler(func):
    """
        _connectionHandler_
    """
    def wrapperFunction(*args, **dictArgs):
        """
        _wrapperFunction_
        """
        try:
            t0astConnection = Connection(**dbConfig)
            reValue = func(t0astConnection, *args, **dictArgs)
            t0astConnection.commit()
            print "sql executed"
            t0astConnection.close()
            print "sql closed"
            return reValue
        except Exception, ex:
            print "Error: %s" % ex
            t0astConnection.close()
    
    return wrapperFunction

class T0TableSetup:
    """
    _TableSetup_ 
    Temporary class for T0AST database initialization for test purpose
    Provides the methods putting dummy values for testing each component in 
    Repecker Package
    """

    def __init__(self, t0astConnection):
        self.t0astConnection = t0astConnection
        self.datasetNamePrefix = "dataset"
        self.algorithmList = ["split", "accumulate"]
        self.lfnPrefix = "/store/data/"
        self.idStartNumber = 100
        self.runIDList = [1400,1401,1402]
        self.lumiIDList = []
        self.numOfDataset = 14
        self.streamerIndexFile = "/home/sryu/test/samples/HLTFromPureRaw.ind"
        self.events = 44 * 32 - 31 # match with the sample index events number
        # this number is used for the number of streamer 
        # due to the restriction of the trigger_segment table. 
        # need to find a better scheme 
        self.numOfTriggSeg = 20 
        for i in range(10):
            self.lumiIDList.append(i)
        self.triggerSegStatusList = [0, 1, 2]
        self.triggerPaths = [
                "HLTriggerFirstPath",
                "HLT2jet",           
                "HLT3jet",           
                "HLT4jet",           
                "HLT2jetAco",        
                "HLT1jet1METAco",    
                "HLT1jet1MET",       
                "HLT2jet1MET",       
                "HLT3jet1MET",       
                "HLT4jet1MET",       
                "HLT1MET1HT"
                ]
        
    def getRunNumbers(self):
        return self.runIDList
     
    def getRandomFileSize(self):
        return random.randint(100, 400)
    
    def getRandomValueFromList(self, list):
        return list[random.randrange(len(list))]
    
    def insertIntoRUN_STATUS(self):
        print "insert run_status"
        statusList = ["Started", "Finished"]
        i = 0
        for status in statusList:
            sqlStr = "INSERT INTO RUN_STATUS VALUES (%d, '%s')" % (i, status)
            self.t0astConnection.execute(sqlStr)
            i = i+1
        
    
    def insertIntoRUN(self):
        #print "insert run"
        for runID in self.runIDList:
            sqlStr = """INSERT INTO RUN (run_id, version, run_status) VALUES (%d, '%s', %d)
                     """ % (runID,"CMSSW_2_0_3", 1)
            self.t0astConnection.execute(sqlStr)
        
    def insertIntoTRIGGER_LABEL(self):
        #print "insert trigger details"
        id = 0;
        for path in self.triggerPaths:
            sqlStr = "INSERT INTO TRIGGER_LABEL VALUES (%d, '%s')" % (
                                   self.idStartNumber + id, path)     
            id = id + 1
            self.t0astConnection.execute(sqlStr)
    
    def insertIntoRUN_TRIG_ASSOC(self):
        #print "insert run_trig_assoc"
        id = 0;
        for path in self.triggerPaths:
            sqlStr = "INSERT INTO RUN_TRIG_ASSOC VALUES (%d, %d)" % (
                                   self.getRandomValueFromList(self.runIDList), 
                                   self.idStartNumber + id)     
            id = id + 1
            self.t0astConnection.execute(sqlStr)
            
    def insertIntoLUMI_SECTION(self):
   
        #print "insert LUMI_SECTION"
        for runID in self.runIDList:
            #print "runID: %d" % runID
            for lumiID in self.lumiIDList:
                #print "lumi ID: %d" % lumiID, 
                sqlStr = "INSERT INTO LUMI_SECTION (lumi_id, run_id) VALUES (%d, %d)"  \
                          % (lumiID, runID)
                self.t0astConnection.execute(sqlStr)
            
                
    def insertIntoSTREAM(self):
        #print "insert into stream"
        sqlStr = "INSERT INTO STREAM VALUES (0, 'Express')"
        self.t0astConnection.execute(sqlStr)
        sqlStr = "INSERT INTO STREAM VALUES (1, 'Calib')"
        self.t0astConnection.execute(sqlStr)
        sqlStr = "INSERT INTO STREAM VALUES (2, 'BulkPhysics')"
        self.t0astConnection.execute(sqlStr)
    
    def insertIntoStreamer(self):
        #print "insert into streamer"
        for id in range(self.numOfTriggSeg):
            sqlStr = """INSERT INTO STREAMER (streamer_id, lumi_id, run_id, 
                        start_time, filesize, events, lfn, indexpfn, streamname, splitable)               
                        VALUES (%d, %d, %d, %d, %d, %d, '%s', '%s', '%s', %d)""" \
                      % (self.idStartNumber + id,
                         self.getRandomValueFromList(self.lumiIDList), 
                         self.getRandomValueFromList(self.runIDList), 
                         1, 
                         self.getRandomFileSize(), 
                         self.events,  
                         self.lfnPrefix + "streamer" + str(id),
                         self.streamerIndexFile,
                         "physics", 
                         1)
            self.t0astConnection.execute(sqlStr)
  
    def insertIntoPRIMARY_DATASET(self):
        #print "insert into primary dataset"
        for id in range(self.numOfDataset):
            sqlStr = "INSERT INTO PRIMARY_DATASET VALUES(%d, '%s', '%s', %d)" \
                      % (self.idStartNumber + id, 
                         self.datasetNamePrefix + str(id),  
                         self.getRandomValueFromList(self.algorithmList),
                         100)
            self.t0astConnection.execute(sqlStr)

    def insertIntoTRIG_DATASET_ASSOC(self):
        #print "insert trig_dataset_assoc"
        id = 0;
        for dataset in range(self.numOfDataset):
            for id in range(1):
                sqlStr = "INSERT INTO TRIG_DATASET_ASSOC VALUES (%d, %d, %d)" % ( 
                                   random.randrange(self.idStartNumber, self.idStartNumber + len(self.triggerPaths)),
                                   self.idStartNumber + dataset,
                                   self.getRandomValueFromList(self.runIDList))     
                self.t0astConnection.execute(sqlStr)
            
    def insertIntoDATASET_RUN_STREAM_ASSOC(self):
        #print "insert dataset_run_stream_assoc"
        id = 0;
        for dataset in range(self.numOfDataset):
            sqlStr = "INSERT INTO DATASET_RUN_STREAM_ASSOC VALUES(%d, %d, %d)" % ( 
                                             self.idStartNumber + dataset, 
                                             self.getRandomValueFromList(self.runIDList),
                                             2)     
            self.t0astConnection.execute(sqlStr)
        
    def insertIntoTRIGGER_SEGMENT(self):
        #print "insert into primary trigger segment"
        for id in range(self.numOfTriggSeg):
            sqlStr = "INSERT INTO TRIGGER_SEGMENT VALUES(%d, %d, %d, %d, %d, %d)" \
                      % (self.idStartNumber + random.randrange(self.numOfDataset),
                         self.idStartNumber + id,
                         self.getRandomValueFromList(self.lumiIDList), 
                         self.getRandomValueFromList(self.runIDList), 
                         self.getRandomFileSize(), 
                         1) #self.getRandomValueFromList(self.triggerSegStatusList))
            self.t0astConnection.execute(sqlStr)
                      
    def insertIntoTRIGGER_SEGMENT_STATUS(self):
        #print "insert into segment status"
        statusList = ["New", "Processing", "Complete"]
        i = 0
        for status in statusList:
            sqlStr = "INSERT INTO TRIGGER_SEGMENT_STATUS VALUES (%d, '%s')" % (i, status)
            self.t0astConnection.execute(sqlStr)
            i = i+1
        
    def insertIntoJOB_STATUS(self):
        #print "insert into job status"
        statusList = ["New", "Used", "Success", "Failure"]
        i = 0
        for status in statusList:
            sqlStr = "INSERT INTO JOB_STATUS VALUES (%d, '%s')" % (i, status)
            self.t0astConnection.execute(sqlStr)
            i = i+1
    
    def insertIntoREPACKED_STATUS(self):
        #print "insert into repacked status"
        statusList = ["Mergeable", "Exportable", "Queued", "Merged", "Exported"]
        i = 0
        for status in statusList:
            sqlStr = "INSERT INTO REPACKED_STATUS VALUES (%d, '%s')" % (i, status)
            self.t0astConnection.execute(sqlStr)
            i = i+1
            
    def insertIntoREPACK_JOB_DEF(self, num=10):
        for id in range(num):
            sqlStr = "INSERT INTO REPACK_JOB_DEF VALUES(%d, %d, %d)" \
                      % (self.idStartNumber + id, 1, int(time.time()))
            self.t0astConnection.execute(sqlStr)

    
    def insertIntoJOB_DATASET_STREAMER_ASSOC(self, num=10, runID=1):
        #need to check constraint
        for id in range(num):
            sqlStr = "INSERT INTO JOB_DATASET_STREAMER_ASSOC VALUES(%d, %d, %d)" \
                      % (self.idStartNumber + id, self.idStartNumber + id, 
                         self.idStartNumber + id)
            self.t0astConnection.execute(sqlStr)
        
    def insertIntoJOB_TRIG_SEG_ASSOC(self, num=10, runID=1):
        #need to check constraint
        for id in range(num):
            sqlStr = "INSERT INTO JOB_TRIG_SEG_ASSOC VALUES(%d, %d)" \
                      % (self.idStartNumber + id, self.idStartNumber + id)
            self.t0astConnection.execute(sqlStr)
    
    def insertIntoJOB_DATASET_ASSOC(self, num=10, runID=1):
        #need to check constraint
        for id in range(num):
            sqlStr = "INSERT INTO JOB_DATASET_ASSOC VALUES(%d, %d)" \
                      % (self.idStartNumber + id, self.idStartNumber + id)
            self.t0astConnection.execute(sqlStr)
                      
    
    def insertIntoJOB_STREAMER_ASSOC(self, num=10):
        for id in range(num):
            sqlStr = "INSERT INTO JOB_STREAMER_ASSOC VALUES(%d, %d)" \
                      % (self.idStartNumber + id, self.idStartNumber + id)
            self.t0astConnection.execute(sqlStr)
                   
    def insertIntoREPACKED(self, num=20):
        
        for id in range(num):
            sqlStr = """ INSERT INTO REPACKED (repacked_id, run_id, filesize, lfn,
                         dataset_id, status, export_status, cksum, events) 
                         VALUES(:p_1, :p_2, :p_3, :p_4, :p_5, :p_6, :p_7, :p_8, :p_9)"""
            params = {'p_1': self.idStartNumber + id, 
                      'p_2': self.getRandomValueFromList(self.runIDList),
                      'p_3': self.getRandomFileSize(),
                      'p_4': "/store/repacked/repackID_%s.root" % (self.idStartNumber + id), 
                      'p_5': self.idStartNumber + id,
                      'p_6': 1,
                      'p_7': 2,
                      'p_8': random.randint(200000,300000),
                      'p_9': 999
                      }
            self.t0astConnection.execute(sqlStr, params)
    
    def insertIntoREPACK_LUMI_ASSOC(self):
        pass
    
    def insertIntoREPACK_STREAMER_ASSOC(self):
        pass
    
    def insertIntoMERGE_JOB_DEF(self):
        pass
    
    def insertIntoMERGE_JOB_REPACK_ASSOC(self):
        pass
    
    def insertIntoREPACKED_MERGE_PARETAGE(self):
        pass
    
    def insertIntoPROMPTRECO_JOB_DEF(self):
        pass
    
    def insertIntoPROMPTRECO_JOB_REPACK_ASSOC(self):
        pass
    
    def insertIntoRECONSTRUCTED(self, num=15):
        for id in range(num):
            sqlStr = """ INSERT INTO RECONSTRUCTED (reconstructed_id, run_id, filesize, lfn,
                         dataset_id, status, export_status, cksum, events) 
                         VALUES(:p_1, :p_2, :p_3, :p_4, :p_5, :p_6, :p_7, :p_8, :p_9)"""
            params = {'p_1': self.idStartNumber + id, 
                      'p_2': self.getRandomValueFromList(self.runIDList),
                      'p_3': self.getRandomFileSize(),
                      'p_4': "/store/reco/reconstructedID_%s.root" % (self.idStartNumber + id), 
                      'p_5': self.idStartNumber + id,
                      'p_6': 1,
                      'p_7': 2,
                      'p_8': random.randint(200000,300000),
                      'p_9': 999
                      }
            self.t0astConnection.execute(sqlStr, params)
            
    def insertIntoREPACKED_RECO_PARENTAGE(self, num=15):
        for id in range(num):
            sqlStr = """ INSERT INTO REPACKED_RECO_PARENTAGE (input_id, output_id) 
                         VALUES(:p_1, :p_2)"""
            params = {'p_1': self.idStartNumber + id, 
                      'p_2': self.idStartNumber + id
                      }
            self.t0astConnection.execute(sqlStr, params)
        
        pass    
        
    def initializeGeneral(self):
        #self.insertIntoRUN_STATUS()
        #self.insertIntoJOB_STATUS()
        #self.insertIntoREPACKED_STATUS()
        #self.insertIntoTRIGGER_SEGMENT_STATUS()
        
        self.insertIntoRUN()
        self.insertIntoTRIGGER_LABEL()
        self.insertIntoRUN_TRIG_ASSOC()
        self.insertIntoPRIMARY_DATASET()
        self.insertIntoTRIG_DATASET_ASSOC()
        self.insertIntoSTREAM()
        self.insertIntoDATASET_RUN_STREAM_ASSOC()
        self.insertIntoLUMI_SECTION()
        self.insertIntoStreamer()
        
    def initializeSegmentInjector(self):            
        self.insertIntoTRIGGER_SEGMENT()
        
    def initializeRepackerScheduler(self):
        self.insertIntoREPACK_JOB_DEF()
        #self.insertIntoJOB_TRIG_SEG_ASSOC()
        self.insertIntoJOB_DATASET_STREAMER_ASSOC()
                         
    def initializeRepackerInjetor(self):
        # setup job Instance table
        # change the status of repack_job_def table       
        pass
    
    def initializeMerger(self):
        #
        # Accountant set up
        self.insertIntoREPACKED()
        self.insertIntoREPACK_LUMI_ASSOC()
        self.insertIntoREPACK_STREAMER_ASSOC()
        # Merger setup
        self.insertIntoMERGE_JOB_DEF()
        self.insertIntoMERGE_JOB_REPACK_ASSOC()
        
        # Accountant setup : insert into repacked ("REPACKED status")
        #self.insertIntoREPACKED()
        
        # updated repack file status to merged according to 
        # the merge_job_repack_assoc
        #self.updateStatusREPACKED() 
        self.insertIntoREPACKED_MERGE_PARETAGE()
        
    def initializePromptReco(self):
        self.insertIntoPROMPTRECO_JOB_DEF()
        self.insertIntoPROMPTRECO_JOB_REPACK_ASSOC()
        
        # Accountant setup
        self.insertIntoRECONSTRUCTED()
        self.insertIntoREPACKED_RECO_PARENTAGE()
        
        
    def initializeRecoMerger(self):
        
        #self.insertIntoRECO_MERGE_PARENTAGE()
        pass    
        
    def clearGeneral(self):
        #tables repacker scheduler inserted
        #self.t0astConnection.execute("DELETE FROM JOB_TRIG_SEG_ASSOC")
        #self.t0astConnection.execute("DELETE FROM JOB_DATASET_ASSOC")
        #self.t0astConnection.execute("DELETE FROM JOB_STREAMER_ASSOC")
        
        tableOrder = [
                      "TRIGGER_SEGMENT",
                      "DATASET_RUN_STREAM_ASSOC",
                      "TRIG_DATASET_ASSOC",
                      "PRIMARY_DATASET",
                      "STREAMER",
                      "STREAM",
                      "LUMI_SECTION",
                      "RUN_TRIG_ASSOC",
                      "TRIGGER_LABEL",
                      "RUN",
                      #"REPACKED_STATUS",
                      #"JOB_STATUS",
                      #"TRIGGER_SEGMENT_STATUS",
                      #"RUN_STATUS"
                      ]
        
        for tableName in tableOrder:
            self.t0astConnection.execute("DELETE FROM %s" % tableName)
    
    def clearRepackerScheduler(self):
        tableOrder = ["JOB_DATASET_STREAMER_ASSOC",
                      "REPACK_JOB_DEF"]
        for tableName in tableOrder:
            self.t0astConnection.execute("DELETE FROM %s" % tableName)
    
        
    
        
    def clearRepackerMerger(self):
        #print "start deleting for Merger"
        tableOrder = ["REPACK_LUMI_ASSOC",
                      "REPACK_STREAMER_ASSOC",
                      "REPACKED_MERGE_PARENTAGE",
                      "MERGE_JOB_REPACK_ASSOC",
                      #"MERGE_JOB_INSTANCE",
                      "MERGE_JOB_DEF",
                      ] 
        for tableName in tableOrder:
            self.t0astConnection.execute("DELETE FROM %s" % tableName)
                         
    def clearRepackerInjector(self):
        #print "start deleting"      
        #self.t0astConnection.execute("DELETE FROM REPACK_JOB_INSTANCE")
        pass
    
    def clearPromptReco(self):
        tableOrder = ["REPACKED_RECO_PARENTAGE",
                      "PROMPTRECO_JOB_REPACK_ASSOC",
                      "PROMPTRECO_JOB_DEF",
                      "REPACKED",
                      "RECONSTRUCTED"
                      ] 
        for tableName in tableOrder:
            self.t0astConnection.execute("DELETE FROM %s" % tableName)
    
    def clearBlockInjector(self):
        
        tableOrder = ["BLOCK_PARENTAGE",
                      "BLOCK_RUN_ASSOC",
                      "BLOCK",
                      "ACTIVE_BLOCK",
                      ] 
        for tableName in tableOrder:
            self.t0astConnection.execute("DELETE FROM %s" % tableName)
    
        
    def clearAllTable(self):
        self.clearBlockInjector()
        self.clearPromptReco()
        self.clearRepackerMerger()
        self.clearRepackerInjector()
        self.clearRepackerScheduler()
        self.clearGeneral()
        
    def setRunConfig(self, filePath):
        
        streams1 = { "physics" : {"dataset1" : self.triggerPaths }}
        runConf1 = RunConfig(1400, "CMSSW_2_0_3", streams1)

        streams2 = { "physics" : {"dataset2" : self.triggerPaths[:6],
                                  "dataset3" : self.triggerPaths[6:]}
                    }
        runConf2 = RunConfig(1401, "CMSSW_2_0_3", streams2 ) 
        
        streams3 = { "physics": {}}
        count = 4
        for path in self.triggerPaths:
            streams3['physics']["dataset%s" % count] = [path] 
            count += 1

        runConf3 = RunConfig(1402, "CMSSW_2_0_3", streams3)
        
        runConfigList = FileBackend(filePath)
        runConfigList.append(runConf1)
        runConfigList.append(runConf2)
        runConfigList.append(runConf3)
        runConfigList.save()
        return
                      
if __name__ == "__main__":
    # create RunConfig file 
    path = "/home/sryu/test/RunConfigs"
    
    @connectionHandler
    def testScheduler(dbCon):
        t0Setup = T0TableSetup(dbCon)
        #t0Setup.setRunConfig(path)
        #print "RunConfig is saved"
        #rc = FileBackend(path)
        #rc.load()
        #print "Run 1400 dataset:\n %s" % rc.getRunConfig(1400).primaryDatasets()
        #print "Run 1401 dataset:\n %s" % rc.getRunConfig(1401).primaryDatasets()
        #print "Run 1402 dataset:\n %s" % rc.getRunConfig(1402).primaryDatasets()
        t0Setup.clearRepackerMerger()
        t0Setup.clearRepackerInjector()
        t0Setup.clearRepackerScheduler()
        t0Setup.clearGeneral()
        
        t0Setup.initializeGeneral()
        t0Setup.initializeSegmentInjector()
        t0Setup.insertIntoREPACK_JOB_DEF()
        
    @connectionHandler    
    def testInjector(dbCon):
        t0Setup = T0TableSetup(dbCon)
        t0Setup.insertIntoJOB_DATASET_STREAMER_ASSOC()
    
        #t0Setup.initializeRepackerScheduler()
        #t0Setup.initializeRepackerInjetor()
    
    @connectionHandler    
    def testMerger(dbCon):
        t0Setup = T0TableSetup(dbCon)
        t0Setup.clearAllTable()
        t0Setup.initializeMerger()
    
    @connectionHandler    
    def testBlockInjector(dbCon):
        t0Setup = T0TableSetup(dbCon)
        t0Setup.clearAllTable()
        t0Setup.initializeMerger()
        
    #testScheduler()
    #testInjector()
    testMerger()
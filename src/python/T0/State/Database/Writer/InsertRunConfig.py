#!/usr/bin/env python
"""
_InsertRunConfig_

Methods for inserting the configuration details for a run into the T0AST
database.
"""

__revision__ = "$Id: InsertRunConfig.py,v 1.55 2009/07/18 15:23:12 hufnagel Exp $"
__version__ = "$Revision: 1.55 $"

import logging

from WMCore.DAOFactory import DAOFactory
from T0 import Globals
from T0.State.Database.Reader import ListDatasets
from T0.State.Database.Writer import InsertDataset


def updateRunTable(dbConn, runNumber, process, acqEra):
    """
    _updateRunTable_

    Update the run table with the process, acquisition era and a default
    value for the reco_started column.  We use the existance/non existance
    of the acquisition era to determine whether or not the run config for
    a run has been loaded into the database.
    """
    sqlQuery = """UPDATE run SET PROCESS = :p_1, ACQ_ERA = :p_2,
                  RECO_STARTED = 0 WHERE RUN_ID = :p_3"""

    bindVars = {"p_1": process, "p_2": acqEra, "p_3": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    return

def insertStream(dbConn, streamName):
    """
    _insertStream_

    Insert a stream into the stream table in T0AST. This will
    work even if the stream already exists inside the table.
    """
    sqlQuery = """INSERT INTO stream (ID, NAME) SELECT stream_SEQ.nextval, :p_1
                  FROM DUAL WHERE NOT EXISTS
                  (SELECT NAME FROM stream WHERE NAME = :p_1)"""
    
    bindVars = {"p_1": streamName}
    dbConn.execute(sqlQuery, bindVars)
    return

def insertTrigger(dbConn, runNumber, triggerName, datasetName):
    """
    _insertTrigger_

    Insert a trigger into the "trigger_label" table.  This will work even if
    the trigger label already exists in the table.  Rows will also be added to
    the "run_trig_assoc" and "trig_dataset_assoc" tables.
    """
    triggerInsert = """INSERT INTO trigger_label (ID, LABEL) SELECT
                       trigger_label_SEQ.nextval, :p_1 FROM DUAL
                       WHERE NOT EXISTS
                       (SELECT LABEL FROM trigger_label WHERE LABEL = :p_1)""" 
    
    bindVars = {"p_1": triggerName}
    dbConn.execute(triggerInsert, bindVars)
    
    runTrigAssoc = """INSERT INTO run_trig_assoc (RUN_ID, TRIGGER_ID)
                      SELECT :p_2, (SELECT ID FROM trigger_label WHERE
                      LABEL = :p_1) FROM DUAL WHERE NOT EXISTS (SELECT RUN_ID
                      FROM run_trig_assoc WHERE RUN_ID = :p_2 AND TRIGGER_ID =
                      (SELECT ID FROM trigger_label WHERE LABEL = :p_1))"""
    
    bindVars = {"p_1": triggerName, "p_2": runNumber}
    dbConn.execute(runTrigAssoc, bindVars)
    
    trigDSetAssoc = """INSERT INTO trig_dataset_assoc (TRIG_ID,
                       PRIMARY_DATASET_ID, RUN_ID) SELECT (SELECT ID FROM
                       trigger_label WHERE LABEL = :p_1), (SELECT ID FROM
                       primary_dataset WHERE NAME = :p_2), :p_3 FROM DUAL WHERE
                       NOT EXISTS (SELECT TRIG_ID FROM trig_dataset_assoc WHERE
                       TRIG_ID = (SELECT ID FROM trigger_label
                       WHERE LABEL = :p_1) AND PRIMARY_DATASET_ID = (SELECT ID
                       FROM primary_dataset WHERE NAME = :p_2) AND RUN_ID = :p_3)"""
    
    bindVars = {"p_1": triggerName, "p_2": datasetName, "p_3": runNumber}
    dbConn.execute(trigDSetAssoc, bindVars)
    return

def updateGlobalTagForRunDataset(dbConn, runNumber, primaryDatasetID,
                                 globalTag):
    """
    _updateGlobalTagForRunDataset_

    Update the global tag for a run and primary dataset.
    """
    sqlQuery = """UPDATE reco_config SET GLOBAL_TAG = :p_1 WHERE RUN_ID = :p_2
                  AND PRIMARY_DATASET_ID = :p_3"""
    bindVars = {"p_1": globalTag, "p_2": runNumber, "p_3": primaryDatasetID}

    dbConn.execute(sqlQuery, bindVars)
    dbConn.commit()
    return

def insertT0Config(dbConn, runNumber, version):
    """
    _insertT0Config_

    Add a row to the t0_config table that associates the run number to the
    version of the offline config.
    """
    sqlQuery = """ INSERT INTO t0_config (run_id, config_version)
                   VALUES (:RUN_ID, :CONFIG_VERSION)
               """
    bindVars = {"RUN_ID": runNumber, "CONFIG_VERSION": version}
    
    dbConn.execute(sqlQuery, bindVars)
    return

def insertCMSSWVersion(dbConn, cmsswVersion):
    """
    _insertCMSSWVersion_

    Add a CMSSW version to the cmssw_version table.  This will succeed even if
    the version already exists inside the cmssw_version table.
    """
    sqlQuery = """INSERT INTO cmssw_version (id, name) SELECT
                  cmssw_version_SEQ.nextval, :p_1 FROM DUAL WHERE NOT EXISTS
                  (SELECT id FROM cmssw_version WHERE name = :p_1)"""
    bindVar = {"p_1": cmsswVersion}

    dbConn.execute(sqlQuery, bindVar)
    return

def insertEventScenario(dbConn, eventScenario):
    """
    _insertEventScenario_
    
    Insert an event scenario so that it can be associated with a particular
    dataset and run.  This will work even if the event scenario already exists
    in the table.
    """
    sqlQuery = """INSERT INTO event_scenario (ID, SCENARIO) SELECT
                  event_scenario_SEQ.nextval, :p_1 FROM DUAL WHERE NOT EXISTS
                  (SELECT SCENARIO FROM event_scenario WHERE SCENARIO = :p_1)"""
    bindVar = {"p_1": eventScenario}
    
    dbConn.execute(sqlQuery, bindVar)
    return
    
def insertProcessingStyle(dbConn, runNumber, streamName, processingStyle):
    """
    _insertProcessingStyle_

    Associate a processing style to a stream and run.  If the processingStyle
    is "Express" a primary dataset will be inserted into T0AST for all the
    data produced for the stream: Stream<streamName>
    """
    if processingStyle == "Express":
        datasetName = "Stream%s" % streamName
        InsertDataset.insertPrimaryDataset(dbConn, datasetName)
        InsertDataset.assocPrimaryDatasetRunStream(dbConn, datasetName,
                                                   streamName, runNumber)

    sqlQuery = """INSERT INTO run_stream_style_assoc
                  (run_id, stream_id, style_id)
                  VALUES (:p_1,
                          (SELECT id FROM stream WHERE name = :p_2),
                          (SELECT id FROM processing_style WHERE name = :p_3))"""

    bindVars = {"p_1": runNumber, "p_2": streamName, "p_3": processingStyle}
    dbConn.execute(sqlQuery, bindVars)
    return

def insertStreamConfig(dbConn, runNumber, streamName, streamConfig):
    """
    _insertStreamConfig_

    Insert the configuration for a stream.  If the stream is to have express
    processing also insert the express processing config.
    """
    insertProcessingStyle(dbConn, runNumber, streamName,
                          streamConfig.ProcessingStyle)

    if streamConfig.ProcessingStyle != "Express":
        return

    sqlQuery = """INSERT INTO express_config
                  (run_id, stream_id, processing_config_url,
                  splitInProcessing, proc_version,
                  alcamerge_config_url, global_tag)
                  VALUES (:RUN_ID,
                          (SELECT id FROM stream WHERE name = :STREAM),
                          :PROC_URL, :SPLIT_IN_PROC, :PROC_VER,
                          :ALCAMERGE_URL, :GLOBAL_TAG)"""

    bindParams = {"RUN_ID": runNumber, "STREAM": streamName,
                  "PROC_URL": streamConfig.ProcessingConfigURL,
                  "ALCAMERGE_URL": streamConfig.AlcaMergeConfigURL,
                  "GLOBAL_TAG": streamConfig.GlobalTag,
                  "SPLIT_IN_PROC": int(streamConfig.SplitInProcessing),
                  "PROC_VER": streamConfig.ProcessingVersion}
    dbConn.execute(sqlQuery, bindParams)

    sqlQuery = """INSERT INTO run_stream_tier_assoc
                  (run_id, stream_id, data_tier_id)
                  VALUES (:RUN_ID,
                          (SELECT id FROM stream WHERE name = :STREAM),
                          (SELECT id FROM data_tier WHERE name = :DATA_TIER))"""

    bindVars = []
    for data_tier in streamConfig.DataTiers:
        bindVars.append({"RUN_ID": runNumber,
                         "STREAM": streamName,
                         "DATA_TIER": data_tier})
    dbConn.execute(sqlQuery, bindVars)

    return

def insertScenarioConfig(dbConn, runNumber, datasetName, eventScenario):
    """
    _insertScenarioConfig_
    
    Associate an event scenario with a particular run number and dataset.  This
    will make sure that the scenario already exists inside the event_scenario
    table before making the association.
    """
    insertEventScenario(dbConn, eventScenario)

    sqlQuery = """ INSERT INTO scenario_config (run_id, primary_dataset_id,
                                                scenario_id)
                   VALUES (:RUN_ID,
                          (SELECT id FROM primary_dataset WHERE name = :DATASET_NAME),
                          (SELECT id FROM event_scenario
                          WHERE scenario = :SCENARIO))
                """
    bindVars = {"RUN_ID": runNumber, "DATASET_NAME": datasetName,
                "SCENARIO": eventScenario}

    dbConn.execute(sqlQuery, bindVars)
    return

def updateRepackExpressVersion(dbConn, runNumber, repackVersion, expressVersion):
    """
    _updateRepackExpressVersion_

    Update repack and or express version for run (if neccessary)
    """
    if repackVersion == None and expressVersion == None:
        return

    bindParams = {}

    sqlQuery = """UPDATE run SET """

    if repackVersion != None:
        sqlQuery += """repack_version = ( SELECT id from cmssw_version WHERE name = :p_1 )"""
        bindParams["p_1"] = repackVersion
        insertCMSSWVersion(dbConn, repackVersion)

    if expressVersion != None:
        if repackVersion != None:
            sqlQuery += """, """
        sqlQuery += """express_version = ( SELECT id from cmssw_version WHERE name = :p_2 )"""
        bindParams["p_2"] = expressVersion
        insertCMSSWVersion(dbConn, expressVersion)

    sqlQuery += """ WHERE run_id = :p_3"""
    bindParams["p_3"] = runNumber

    dbConn.execute(sqlQuery, bindParams)

    return
    
def insertRepackConfig(dbConn, runNumber, datasetName, processingVersion):
    """
    _insertRepackConfig_
    
    Set the repacked config for a given run number and dataset.  
    """
    sqlQuery = """ INSERT INTO repack_config (run_id, primary_dataset_id,
                                              proc_version)
                   VALUES (:RUN_ID, 
                           (SELECT id FROM primary_dataset WHERE name = :DATASET_NAME),
                           :PROC_VERSION)
               """
    bindVars = {"RUN_ID": runNumber, "DATASET_NAME": datasetName,
                "PROC_VERSION": processingVersion}
    
    dbConn.execute(sqlQuery, bindVars)
    return

def insertRecoConfig(dbConn, runNumber, datasetName, recoEnabled, cmsswVersion,
                     configURL, processingVersion, globalTag):
    """
    _insertRecoConfig_
    
    Set the reco config for a given run and dataset.  Currently, reco can be
    enabled/disabled, and the framework version, processing version and config
    URL can be set.  The global tag is set later on in the run using
    updateGlobalTagForRunDataset().
    """
    insertCMSSWVersion(dbConn, cmsswVersion)
    
    sqlQuery = """ INSERT INTO reco_config (run_id, primary_dataset_id, do_reco,
                           cmssw_version_id, global_tag, config_url, proc_version)
                   VALUES (:RUN_ID,
                           (SELECT id FROM primary_dataset  WHERE name = :DATASET_NAME),
                           :DO_RECO, 
                           (SELECT id FROM cmssw_version 
                            WHERE name = :CMSSW_VERSION),
                           :GLOBAL_TAG,
                           :CONFIG_URL,
                           :PROC_VERSION)
               """
    bindVars = {"RUN_ID": runNumber, "DATASET_NAME": datasetName,
                "DO_RECO": int(recoEnabled), "CMSSW_VERSION": cmsswVersion,
                "CONFIG_URL": configURL, "PROC_VERSION": processingVersion,
                "GLOBAL_TAG": globalTag}
                
    dbConn.execute(sqlQuery, bindVars)
    return

def insertAlcaConfig(dbConn, runNumber, datasetName, alcaEnabled, cmsswVersion,
                     configURL, processingVersion):    
    """
    _insertAlcaConfig_
    
    Set the alca config for a given run and dataset.  Currently, alca can be
    enabled/disabled, and the framework version, processing version and config
    URL can be set.  
    """
    insertCMSSWVersion(dbConn, cmsswVersion)
    
    sqlQuery = """ INSERT INTO alca_config (run_id, primary_dataset_id, do_alca,
                           cmssw_version_id, config_url, proc_version)
                   VALUES (:RUN_ID,
                           (SELECT id FROM primary_dataset 
                            WHERE name = :DATASET_NAME), 
                           :DO_ALCA, 
                           (SELECT id FROM cmssw_version 
                            WHERE name = :CMSSW_VERSION),
                           :CONFIG_URL,
                           :PROC_VERSION)
               """
    bindVars = {"RUN_ID": runNumber, "DATASET_NAME": datasetName,
                "DO_ALCA": int(alcaEnabled), "CMSSW_VERSION": cmsswVersion,
                "CONFIG_URL": configURL, "PROC_VERSION": processingVersion}

    dbConn.execute(sqlQuery, bindVars)
    return
    
def insertWMBSPublishConfig( dbConn, runNumber, datasetName, wmbsPublishEnabled,
                            dataTiersTo ):    
    """
    _insertWMBSPublishConfig_
    
    Set the WMBS Publish config for a given run and dataset.
    """
    
    sqlQuery = """ INSERT INTO wmbs_publish_config (run_id, primary_dataset_id,
                                             do_wmbs_publish, data_tiers_to )
                   VALUES (:RUN_ID,
                           (SELECT id FROM primary_dataset 
                            WHERE name = :DATASET_NAME), 
                           :DO_WMBS_PUBLISH, 
                           :DATA_TIERS_TO)
               """
    # convert the data tier strings into ids
    dataTierMap = ListDatasets.listDataTiersByNames( dbConn )
    dataTiersToString = ''
    for dataTierTo in dataTiersTo:
        (dataTiers,database) = dataTierTo.split('@')
        dataTiersString = ''
        for dataTier in dataTiers.split(','):
            dataTiersString += str( dataTierMap[dataTier] ) + ','
        dataTiersToString += dataTiersString.rstrip(',') + '@' + database + ' '
    bindVars = {"RUN_ID": runNumber, "DATASET_NAME": datasetName,
                "DO_WMBS_PUBLISH": int(wmbsPublishEnabled),
                "DATA_TIERS_TO": dataTiersToString.rstrip()}
    
    dbConn.execute(sqlQuery, bindVars)
    return
    
def insertDQMConfig(dbConn, runNumber, datasetName, dqmEnabled, cmsswVersion,
                     configURL, processingVersion):    
    """
    _insertDQMConfig_
    
    Set the DQM config for a given run and dataset.  Currently, DQM can be
    enabled/disabled, and the framework version, processing version and config
    URL can be set.  
    """
    insertCMSSWVersion(dbConn, cmsswVersion)
    
    sqlQuery = """ INSERT INTO dqm_config (run_id, primary_dataset_id, do_dqm,
                           cmssw_version_id, config_url, proc_version)
                   VALUES (:RUN_ID,
                           (SELECT id FROM primary_dataset 
                            WHERE name = :DATASET_NAME), 
                           :DO_DQM, 
                           (SELECT id FROM cmssw_version 
                            WHERE name = :CMSSW_VERSION),
                           :CONFIG_URL,
                           :PROC_VERSION)
               """
    bindVars = {"RUN_ID": runNumber, "DATASET_NAME": datasetName,
                "DO_DQM": int(dqmEnabled), "CMSSW_VERSION": cmsswVersion,
                "CONFIG_URL": configURL, "PROC_VERSION": processingVersion}
    
    dbConn.execute(sqlQuery, bindVars)
    return

def insertTier1Skims(dbConn, runNumber, tier1Skims):
    """
    _insertTier1Skims_

    Insert all the Tier1Skims that are supposed to run over a particular data
    tier and primary dataset into the t1skim_config table in T0AST.
    """
    sqlQuery = """INSERT INTO t1skim_config (run_id, primary_dataset_id,
                                             data_tier_id, cmssw_version_id,
                                             two_file_read, skim_name,
                                             config_url, proc_version)
                  SELECT :RUN_ID,
                   (SELECT id FROM primary_dataset WHERE name = :DATASET_NAME),
                   (SELECT id FROM data_tier WHERE name = :DATA_TIER),
                   (SELECT id FROM cmssw_version WHERE name = :CMSSW_VERSION),
                   :TWO_FILE_READ, :SKIM_NAME, :CONFIG_URL, :PROC_VERSION
                   FROM dual
               """
    
    for tier1Skim in tier1Skims:
        insertCMSSWVersion(dbConn, tier1Skim.CMSSWVersion)
    
        bindVars = {"RUN_ID": runNumber,
                    "DATASET_NAME": tier1Skim.PrimaryDataset,
                    "DATA_TIER": tier1Skim.DataTier,
                    "CMSSW_VERSION": tier1Skim.CMSSWVersion,
                    "TWO_FILE_READ": int(tier1Skim.TwoFileRead),
                    "SKIM_NAME": tier1Skim.SkimName,
                    "CONFIG_URL": tier1Skim.ConfigURL,
                    "PROC_VERSION": tier1Skim.ProcessingVersion}
        dbConn.execute(sqlQuery, bindVars)
        
    return

def insertStorageNode(dbConn, nodeName):
    """
    _insertStorageNode_

    Insert a PhEDEx storage node into the storage node table.  This will
    succeed even if the node already exists.  Also insert the storage node
    name as a location in WMBS.
    """
    sqlQuery = """INSERT INTO storage_node (id, name)
                  SELECT storage_node_SEQ.nextval, :p_1 FROM DUAL
                  WHERE NOT EXISTS (SELECT name FROM storage_node
                  WHERE name = :p_1)"""
    bindVars = {"p_1": nodeName}
    dbConn.execute(sqlQuery, bindVars)

    daoFactory = DAOFactory(package="WMCore.WMBS", logger = logging,
                    dbinterface = dbConn.getDBInterface())
    locationAction = daoFactory(classname = "Locations.New")
    locationAction.execute(sename = nodeName)    
    return

def insertPhEDExConfig(dbConn, runNumber, primaryDatasetName, custodialNode,
                       archivalNode, custodialPriority, custodialAutoApprove):
    """
    _insertPhEDExConfig_

    Insert the phedex subscriptions for the given primary dataset.  This
    implements the following policy:
      https://twiki.cern.ch/twiki/bin/view/CMS/T0ASTDiscussExportPolicy
    """
    sqlQuery = """INSERT INTO phedex_subscription (run_id, primary_dataset_id,
                                                   data_tier_id, node_id, 
                                                   custodial_flag, request_only,
                                                   priority)
                  VALUES (:runNumber,
                          (SELECT id FROM primary_dataset WHERE name = :pDatasetName),
                          (SELECT id FROM data_tier WHERE name = :dataTierName),
                          (SELECT id FROM storage_node WHERE name = :storageNode),
                          :custodial, :autoApprove, :priority)""" 

    bindVars = {"runNumber": runNumber, "pDatasetName": primaryDatasetName}
    if custodialNode != None:
        insertStorageNode(dbConn, custodialNode)
        
        bindVars["storageNode"] = custodialNode
        bindVars["priority"] = custodialPriority
        bindVars["custodial"] = 1

        if custodialAutoApprove == True:
            bindVars["autoApprove"] = "n"
        else:
            bindVars["autoApprove"] = "y"

        bindVars["dataTierName"] = "RAW"
        dbConn.execute(sqlQuery, bindVars)
        bindVars["dataTierName"] = "RECO"
        dbConn.execute(sqlQuery, bindVars)
        bindVars["dataTierName"] = "AOD"
        dbConn.execute(sqlQuery, bindVars)

    if archivalNode != None:
        insertStorageNode(dbConn, archivalNode)
        
        bindVars["storageNode"] = archivalNode
        bindVars["autoApprove"] = "n"
        bindVars["priority"] = "high"
        bindVars["custodial"] = 0

        bindVars["dataTierName"] = "RAW"
        dbConn.execute(sqlQuery, bindVars)
        bindVars["dataTierName"] = "RECO"
        dbConn.execute(sqlQuery, bindVars)
        bindVars["dataTierName"] = "AOD"
        dbConn.execute(sqlQuery, bindVars)
        bindVars["dataTierName"] = "ALCARECO"
        dbConn.execute(sqlQuery, bindVars)        
    
    return

def insertRunConfig(dbConn, runNumber, process, mapping, acqEra):
    """
    _insertRunConfig_

    Write a run configuration to T0AST

    This includes CMSSW version that data was taken with, stream to
    primary dataset to trigger path mapping and the online process name

    If something happens and the RunConfig data can't be inserted into T0AST
    a RuntimeError will be thrown and everything will be rolled back.
    """
    logging.debug("insertRunConfig(): Running %s" % runNumber)
    
    try:
        updateRunTable(dbConn, runNumber, process, acqEra)
    except Exception, ex:
        errorMsg = "Failed to update run '%s' with process '%s'" % \
                   (runNumber, process)
        errorMsg += "Received error: %s" % ex
        logging.debug(errorMsg)

        dbConn.rollback()
        raise RuntimeError, errorMsg

    for stream in mapping.iterkeys():
        try:
            insertStream(dbConn, stream)
        except Exception, ex:
            errorMsg = "Failed to insert stream '%s' for run '%s'. " % \
                       (stream, runNumber)
            errorMsg += "Received error: %s" % ex
            logging.debug(errorMsg)

            dbConn.rollback()
            raise RuntimeError, errorMsg

        for dataset in mapping[stream].iterkeys():
            try:
                InsertDataset.insertPrimaryDataset(dbConn, dataset)
                InsertDataset.assocPrimaryDatasetRunStream(dbConn, dataset,
                                                           stream, runNumber)
            except Exception, ex:
                errorMsg = "Failed to insert dataset '%s' for run '%s', " % \
                           (dataset, runNumber)
                errorMsg = "stream '%s'." % stream
                errorMsg = "Received error: %s" % ex
                logging.debug(errorMsg)

                dbConn.rollback()
                raise RuntimeError, errorMsg

            for trigger in mapping[stream][dataset]:
                try:
                    insertTrigger(dbConn, runNumber, trigger, dataset)
                except Exception, ex:
                    errorMsg = "Failed to insert trigger '%s' for run '%s'," % \
                               (trigger, runNumber)
                    errorMsg = " dataset '%s'." % stream
                    errorMsg = "Received error: %s" % ex
                    logging.debug(errorMsg)
                    
                    dbConn.rollback()
                    raise RuntimeError, errorMsg
    return

def insertDatasetConfig(dbConn, runNumber, datasetConfig):
    """
    _insertDatasetConfig_

    Insert configuration information into T0AST for a given dataset and
    run number.  The datasetConfig object should have sections for Repacking,
    Reco, Alca, DQM and Skims.
    """
    insertScenarioConfig(dbConn, runNumber, datasetConfig.Name,
                         datasetConfig.Scenario)
        
    insertRepackConfig(dbConn, runNumber, datasetConfig.Name,
                       datasetConfig.Repack.ProcessingVersion)

    insertRecoConfig(dbConn, runNumber, datasetConfig.Name,
                     datasetConfig.Reco.DoReco, datasetConfig.Reco.CMSSWVersion,
                     datasetConfig.Reco.ConfigURL,
                     datasetConfig.Reco.ProcessingVersion,
                     datasetConfig.Reco.GlobalTag)

    insertAlcaConfig(dbConn, runNumber, datasetConfig.Name,
                     datasetConfig.Alca.DoAlca, datasetConfig.Alca.CMSSWVersion,
                     datasetConfig.Alca.ConfigURL,
                     datasetConfig.Alca.ProcessingVersion)

    insertWMBSPublishConfig(dbConn, runNumber, datasetConfig.Name,
                            datasetConfig.WMBSPublish.DoWMBSPublish,
                            datasetConfig.WMBSPublish.DataTiersTo)

    insertDQMConfig(dbConn, runNumber, datasetConfig.Name,
                    datasetConfig.DQM.DoDQM, datasetConfig.DQM.CMSSWVersion,
                    datasetConfig.DQM.ConfigURL,
                    datasetConfig.DQM.ProcessingVersion)

    insertPhEDExConfig(dbConn, runNumber, datasetConfig.Name,
                       datasetConfig.CustodialNode, datasetConfig.ArchivalNode,
                       datasetConfig.CustodialPriority,
                       datasetConfig.CustodialAutoApprove)

    insertTier1Skims(dbConn, runNumber, datasetConfig.Tier1Skims)    
    return

def updatePSetHash(dbConn, runNumber, datasetID, dataTier, hashValue):
    """
    _updatePSetHash_

    Update the PSet hash value in config table.
    The value shouldn't be changed once updated
    
    To do: Should Add sanity check either here? hashValue should be same for
           the same runNumber and datasetID pair
    """
    if dataTier == "RECO" or dataTier == "ALCARECO" or dataTier == "AOD":
        tableName = "reco_config"
    elif dataTier == "ALCA":
        tableName = "alca_config"
    elif dataTier == "DQM":
        tableName = "dqm_config"
        
    sqlQuery = """UPDATE %s SET PSET_HASH = :p_1 WHERE RUN_ID = :p_2
                  AND PRIMARY_DATASET_ID = :p_3 AND PSET_HASH IS NULL""" % tableName
                  
    bindVars = {"p_1": hashValue, "p_2": runNumber, "p_3": datasetID}

    dbConn.execute(sqlQuery, bindVars)
    return

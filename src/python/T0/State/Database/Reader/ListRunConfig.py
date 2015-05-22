#!/usr/bin/env python
"""
_ListRunConfig_

Utilities for retrieving RunConfig information from T0AST.
"""

__revision__ = "$Id: ListRunConfig.py,v 1.53 2009/07/18 15:23:12 hufnagel Exp $"
__version__ = "$Revision: 1.53 $"

def runExists(dbConn, runNumber):
    """
    _runExists_

    Check if a run exists in T0AST.  A row is added to the run table by the
    transfer system, but the ACQ_ERA column is empty.  This is what we
    use to decide if a RunConfig needs to be inserted for a run.
    """
    sqlQuery = "SELECT ACQ_ERA FROM run WHERE RUN_ID = :p_1"
    bindVars = {"p_1": runNumber}

    dbConn.execute(sqlQuery, bindVars)
    runExistsResult = dbConn.fetchall()

    if runExistsResult == []:
        return False

    if runExistsResult[0][0] == None:
        return False

    return True

def retrieveStreamToDatasets(dbConn, runNumber):
    """
    _retrieveStreamDatasetMapping_

    Query T0AST for the mapping of streams to datasets for a given run.
    """
    sqlQuery = """SELECT a.ID, b.PRIMARY_DATASET_ID FROM stream a
                  INNER JOIN dataset_run_stream_assoc b
                  ON a.ID = b.STREAM_ID
                  INNER JOIN trig_dataset_assoc c
                  ON c.PRIMARY_DATASET_ID = b.PRIMARY_DATASET_ID WHERE
                  b.RUN_ID = :p_1
                  GROUP BY a.ID, b.PRIMARY_DATASET_ID"""

    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)

    # rows with 1st element the stream name, 2nd element the dataset id
    # now convert into dictionary
    streamToDatasets = {}
    for row in dbConn.fetchall():
        if streamToDatasets.has_key(row[0]):
            streamToDatasets[row[0]].append(row[1])
        else:
            streamToDatasets[row[0]] = [row[1]]

    return streamToDatasets

def retrieveDatasetToTriggers(dbConn, runNumber):
    """
    _retrieveDatasetToTriggers_

    Query T0AST for the mapping of datasets to triggers for a given run.
    """
    sqlQuery = """SELECT trig_dataset_assoc.PRIMARY_DATASET_ID,
                  trigger_label.LABEL FROM trig_dataset_assoc INNER JOIN
                  trigger_label ON trig_dataset_assoc.TRIG_ID = trigger_label.ID
                  WHERE trig_dataset_assoc.RUN_ID = :p_1 GROUP BY
                  trig_dataset_assoc.PRIMARY_DATASET_ID, trigger_label.LABEL"""

    bindVars = {"p_1" : runNumber}
    dbConn.execute(sqlQuery, bindVars)

    # rows with 1st element the dataset id, 2nd the trigger label
    # now convert into dictionary

    datasetToTriggers = {}
    for row in dbConn.fetchall():
        if datasetToTriggers.has_key(row[0]):
            datasetToTriggers[row[0]].append(row[1])
        else:
            datasetToTriggers[row[0]] = [row[1]]

    return datasetToTriggers

def retrievePrimaryDatasets(dbConn, runNumber):
    """
    _retrievePrimaryDatasets_

    Query T0AST for dataset ids and names for this run.
    """
    sqlQuery = """SELECT ID, primary_dataset.NAME FROM
                  primary_dataset INNER JOIN dataset_run_stream_assoc
                  ON primary_dataset.ID = dataset_run_stream_assoc.PRIMARY_DATASET_ID
                  WHERE dataset_run_stream_assoc.RUN_ID = :p_1"""

    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)

    # rows with 1st element the dataset name, 2nd the dataset id
    # now convert into dictionary

    primaryDatasets = {}
    for row in dbConn.fetchall():
        primaryDatasets[row[0]] = row[1]

    return primaryDatasets

def retrieveStreams(dbConn, runNumber):
    """
    _retrieveStreams_

    Query T0AST for stream ids and names for this run.
    """
    sqlQuery = """SELECT stream.ID, stream.NAME FROM stream INNER JOIN
                  dataset_run_stream_assoc ON stream.ID =
                  dataset_run_stream_assoc.stream_id
                  WHERE dataset_run_stream_assoc.run_id = :p_1"""

    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)

    # rows with 1st element the stream name, 2nd the stream id
    # now convert into dictionary
    streams = {}
    for row in dbConn.fetchall():
        streams[row[0]] = row[1]

    return streams

def retrieveVersionAndHLTKey(dbConn, runNumber):
    """
    _retrieveVersionAndHLTKey_

    Retrieve the Online version and HLT key for a run
    """
    sqlQuery = """SELECT b.name, a.hltkey FROM run a
                  INNER JOIN cmssw_version b
                  ON b.id = a.run_version
                  WHERE a.run_id = :p_1"""

    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    return dbConn.fetchall()

def retrieveRunInfo(dbConn, runNumber):
    """
    _retrieveRunInfo_

    Retrieve the CMSSW version, process name, start time, acquisition era
    and processing version from the run table.
    """
    sqlQuery = """SELECT b.name, c.name, a.process, a.start_time, a.acq_era FROM run a
                  INNER JOIN cmssw_version b
                  ON b.id = a.repack_version
                  INNER JOIN cmssw_version c
                  ON c.id = a.express_version
                  WHERE a.run_id = :p_1"""
    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    return dbConn.fetchall();

def retrieveScenario(dbConn, runNumber):
    """
    _retrieveScenario_

    Retrieve scenario name from event_scenario and scenario_config table
    """
    sqlQuery = """ SELECT primary_dataset_id, scenario FROM scenario_config
                      INNER JOIN event_scenario ON scenario_id = id
                      WHERE run_id = :p_1 """

    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    scenarios = {}

    for row in dbConn.fetchall():
        scenarios[row[0]] = row[1]
    return scenarios

def retrieveRepackConfig(dbConn, runNumber):
    """
    _retrieveRepackConfig_ 
    
    Retrieve data from repack_config and the repacker version from run
    Return as dict of datasets and processing versions
    """
    sqlQuery = """SELECT primary_dataset_id, proc_version FROM repack_config
                  WHERE run_id = :p_1"""
    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    t0RepackConfigs = {}
    for row in results:
        t0RepackConfigs[row[0]] = {}
        t0RepackConfigs[row[0]]["PROC_VER"] = row[1]
    
    return t0RepackConfigs
        
def retrieveRecoConfig(dbConn, runNumber):
    """
    _retrieveRecoConfig_ 
    
    Retrieve data from repack_config
    organize as dict
    
    [dataset_id][parameter] = value
    
    parameters
    ["DO_RECO"]  : boolean True, False
    ["GLOBALTAG"] : String
    ["CMSSW_VERSION"] : String
    ["CONFIG_URL"] : String
    ["PROC_VER"] : String
    """
    
    sqlQuery = """ SELECT primary_dataset_id, do_reco, global_tag, name,
                   config_url, proc_version FROM reco_config
                   INNER JOIN cmssw_version ON cmssw_version_id = id 
                   WHERE run_id = :p_1 """ 
    
    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    t0RecoConfigs = {}
    
    for row in dbConn.fetchall():
        t0RecoConfigs[row[0]] = {}
        t0RecoConfigs[row[0]]["DO_RECO"] = bool(row[1])
        t0RecoConfigs[row[0]]["GLOBALTAG"] = row[2]
        t0RecoConfigs[row[0]]["CMSSW_VERSION"] = row[3]
        t0RecoConfigs[row[0]]["CONFIG_URL"] = row[4]
        t0RecoConfigs[row[0]]["PROC_VER"] = row[5]
    
    return t0RecoConfigs

def retrieveRecoConfigForDataset(dbConn, runNumber, primaryDatasetName):
    """
    _retrieveRecoConfigForDataset_

    Retrieve the configuration that was used for reconstruction for a given
    run and dataset.  The configuration is returned in the form of a dictionary
    with the following keys:
      DO_RECO - Bool, True means reconstruction was preformed
      GLOBAL_TAG - Global tag that was used
      CMSSW_VERSION - Framework version used
      CONFIG_URL - URL to the framework config
      PROC_VER - Processing version
    """
    sqlQuery = """SELECT DO_RECO, GLOBAL_TAG, cmssw_version.NAME, CONFIG_URL,
                  PROC_VERSION FROM reco_config INNER JOIN cmssw_version ON
                  reco_config.CMSSW_VERSION_ID = cmssw_version.ID INNER JOIN
                  primary_dataset ON primary_dataset.ID =
                  reco_config.PRIMARY_DATASET_ID WHERE RUN_ID = :p_1 AND
                  primary_dataset.NAME = :p_2""" 
    
    bindVars = {"p_1": runNumber, "p_2": primaryDatasetName}
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()
    
    recoConfig = {}
    recoConfig["DO_RECO"] = bool(results[0][0])
    recoConfig["GLOBAL_TAG"] = results[0][1]
    recoConfig["CMSSW_VERSION"] = results[0][2]
    recoConfig["CONFIG_URL"] = results[0][3]
    recoConfig["PROC_VER"] = results[0][4]
    
    return recoConfig
    
def retrieveAlcaConfig(dbConn, runNumber):
    """
    _retrieveAlcaConfig_ 
    
    Retrieve data from alca_config
    organize as dict
    
    [dataset_id][parameter] = value
    
    parameters
    ["DO_ALCA"]  : boolean True, False 
    ["CMSSW_VERSION"] : String
    ["CONFIG_URL"] : String
    ["PROC_VER"] : String
    """
    
    sqlQuery = """ SELECT primary_dataset_id, do_alca, name, 
                          config_url, proc_version FROM alca_config
                   INNER JOIN cmssw_version ON cmssw_version_id = id 
                   WHERE run_id = :p_1 """ 
    
    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    t0AlcaConfigs = {}
    
    for row in dbConn.fetchall():
        t0AlcaConfigs[row[0]] = {}
        t0AlcaConfigs[row[0]]["DO_ALCA"] = bool(row[1])
        t0AlcaConfigs[row[0]]["CMSSW_VERSION"] = row[2]
        t0AlcaConfigs[row[0]]["CONFIG_URL"] = row[3]
        t0AlcaConfigs[row[0]]["PROC_VER"] = row[4]        
    
    return t0AlcaConfigs
        
def retrieveWMBSPublishConfig(dbConn, runNumber):
    """
    _retrieveWMBSPublishConfig_ 
    
    Retrieve data from wmbs_publish_config
    organize as dict
    
    [dataset_id][parameter] = value
    
    parameters
    ["DO_WMBS_PUBLISH"]  : boolean True, False 
    ["DATA_TIERS_TO"] : List ( of <DataTier>[,DataTier]@<location> )
    """
    
    sqlQuery = """ SELECT primary_dataset_id, do_wmbs_publish,
                          data_tiers_to
                   FROM wmbs_publish_config
                   WHERE run_id = :p_1 """ 
    
    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    t0WMBSPublishConfigs = {}
    
    for row in dbConn.fetchall():
        t0WMBSPublishConfigs[row[0]] = {}
        t0WMBSPublishConfigs[row[0]]["DO_WMBS_PUBLISH"] = bool(row[1])
        dataTiersTo = []
        if row[2]:
            dataTiersTo = row[2].split()
        t0WMBSPublishConfigs[row[0]]["DATA_TIERS_TO"] = dataTiersTo
    
    return t0WMBSPublishConfigs

def retrieveDQMConfig(dbConn, runNumber):
    """
    _retrieveDQMConfig_ 
    
    Retrieve data from alca_config
    organize as dict
    
    [dataset_id][parameter] = value
    
    parameters
    ["DO_DQM"]  : boolean True, False 
    ["CMSSW_VERSION"] : String
    ["CONFIG_URL"] : String
    ["PROC_VER"] : String
    """
    sqlQuery = """ SELECT primary_dataset_id, do_dqm, name, config_url,
                   proc_version FROM dqm_config
                   INNER JOIN cmssw_version ON cmssw_version_id = id 
                   WHERE run_id = :p_1 """ 
    
    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    t0DQMConfigs = {}
    
    for row in dbConn.fetchall():
        t0DQMConfigs[row[0]] = {}
        t0DQMConfigs[row[0]]["DO_DQM"] = bool(row[1])
        t0DQMConfigs[row[0]]["CMSSW_VERSION"] = row[2]
        t0DQMConfigs[row[0]]["CONFIG_URL"] = row[3]
        t0DQMConfigs[row[0]]["PROC_VER"] = row[4]
    
    return t0DQMConfigs       

def retrieveTier1SkimConfig(dbConn, runNumber):
    """
    _retrieveTier1SkimConfig_

    Retrieve all the Tier1 Skim configurations for a given run.  The skims are
    returned in the form of a two level dictionary where the primary dataset ID
    is the first key and the data tier ID is the second key.
    """
    sqlQuery = """SELECT primary_dataset_id, data_tier_id, cmssw_version.name,
                  two_file_read, skim_name, config_url, proc_version
                  FROM t1skim_config INNER JOIN cmssw_version
                  ON cmssw_version.id = cmssw_version_id
                  WHERE run_id = :p_1"""
    
    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)

    skimConfigs = {}
    for skimConfig in dbConn.fetchall():
        if skimConfig[0] not in skimConfigs.keys():
            skimConfigs[skimConfig[0]] = {}
        if skimConfig[1] not in skimConfigs[skimConfig[0]]:
            skimConfigs[skimConfig[0]][skimConfig[1]] = []

        newConfig = {}
        newConfig["CMSSW_VERSION"] = skimConfig[2]
        newConfig["TWO_FILE_READ"] = bool(skimConfig[3])
        newConfig["SKIM_NAME"] = skimConfig[4]
        newConfig["CONFIG_URL"] = skimConfig[5]
        newConfig["PROC_VER"] = skimConfig[6]

        skimConfigs[skimConfig[0]][skimConfig[1]].append(newConfig)
        
    return skimConfigs

def retrieveExpressConfig(dbConn, runNumber):
    """
    _retrieveExpressConfig_

    Retrieve all the express configurations for a given run.  The results are
    returned in the form of a dictionary that is keyed by stream id.
    """
    sqlQuery = """SELECT a.stream_id, a.processing_config_url, a.global_tag,
                         a.splitInProcessing, a.proc_version,
                         a.alcamerge_config_url, c.name FROM express_config a
                  INNER JOIN run_stream_tier_assoc b
                  ON ( b.run_id = a.run_id AND b.stream_id = a.stream_id )
                  INNER JOIN data_tier c
                  ON c.id = b.data_tier_id
                  WHERE a.run_id = :p_1"""
    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)

    expressConfigs = {}
    for result in dbConn.fetchall():
        stream_id = result[0]
        if expressConfigs.has_key(stream_id):
            expressConfig = expressConfigs[stream_id]
            expressConfig["DATATIERS"].append(result[6])
        else:
            expressConfig = {}
            
            expressConfig["PROC_URL"] = result[1]
            expressConfig["GLOBAL_TAG"] = result[2]
            expressConfig["SPLIT_IN_PROC"] = result[3]
            expressConfig["PROC_VER"] = result[4]
            expressConfig["ALCAMERGE_URL"] = result[5]
            expressConfig["DATATIERS"] = [ result[6] ]
            expressConfigs[result[0]] = expressConfig

    return expressConfigs

def retrieveProcessingStyles(dbConn, runNumber):
    """
    _retrieveProcessingStyles_

    Retrieve the stream ID to processing style mappins for the given run.
    """
    sql = """SELECT a.stream_id, b.name FROM run_stream_style_assoc a
             INNER JOIN processing_style b
             ON b.id = a.style_id
             WHERE run_id = :p_1"""

    bindVars = {"p_1": runNumber}
    dbConn.execute(sql, bindVars)

    processingStyle = {}
    for result in dbConn.fetchall():
        processingStyle[result[0]] = result[1] 

    return processingStyle

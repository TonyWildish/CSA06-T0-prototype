#!/usr/bin/env python
"""
_ListDatasets_

Utilities to retrieve information from the dataset tables in T0AST.
"""

__revision__ = "$Id: ListDatasets.py,v 1.26 2009/06/05 21:48:09 gowdy Exp $"
__version__ = "$Revision: 1.26 $"

def listDatasetsForRepackJob(dbConn, jobID):
    """
    _listDatasetsForRepackJob_

    Retrieve a list of the dataset IDs that are expected to be output by a
    particular repack job.
    """
    sqlQuery = """SELECT PRIMARY_DATASET_ID FROM job_streamer_dataset_assoc WHERE
                  JOB_ID = :p_1"""

    bindVars = {"p_1": jobID}
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    datasetIDs = []
    for result in results:
        datasetIDs.append(result[0])
            
    return set(datasetIDs)

def listProcessedDatasets(dbConn):
    """
    _listProcessedDatasets_

    Retrieve a list of all the processed datasets in T0AST.  The results are
    returned in the form of a dictionary where the key is the processed dataset
    id and the value is the processed dataset name.
    """
    sqlQuery = "SELECT id, name FROM processed_dataset"

    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()

    processedDatasets = {}
    for result in results:
        processedDatasets[result[0]] = result[1]

    return processedDatasets

def listDataTiers(dbConn):
    """
    _listDataTiers_

    Retrieve a list of all the data tiers in T0AST.  The results are returned
    in the form of a dictionary where the key is the data tier id and the value
    is the data tier name.
    """
    sqlQuery = "SELECT id, name FROM data_tier"

    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()

    dataTiers = {}
    for result in results:
        dataTiers[result[0]] = result[1]

    return dataTiers

def listDataTiersByNames(dbConn):
    """
    _listDataTiersByNames_

    Retrieve a list of all the data tiers in T0AST.  The results are returned
    in the form of a dictionary where the key is the data tier name and the value
    is the data tier id.
    """
    sqlQuery = "SELECT name, id FROM data_tier"

    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()

    dataTiers = {}
    for result in results:
        dataTiers[result[0]] = result[1]

    return dataTiers

def listRunDatasetMapping(dbConn):
    """
    _listRunDatasetMapping_

    Retrieve a list of all the runs in T0AST and the datasets that are
    associated with them.
    """
    sqlQuery = """SELECT primary_dataset_id, run_id, name from
                  dataset_run_stream_assoc INNER JOIN primary_dataset ON
                  primary_dataset.id = dataset_run_stream_assoc.primary_dataset_id"""
                
    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()

    datasetInfoList = []
    for result in results:
        datasetInfo ={}
        datasetInfo["DATASET_ID"] = result[0]
        datasetInfo["RUN_ID"] = result[1]
        datasetInfo["NAME"] = result[2]
        
        datasetInfoList.append(datasetInfo)

    return datasetInfoList

def listProcessedDatasetNameFromID(dbConn, procds_id):
    """
    _listProcessedDatasetNameFromID_

    Given the ID of a processed dataset return it's name.
    """
    sqlQuery = """SELECT a.NAME from processed_dataset a
                  WHERE a.ID = :p_1"""

    bindVars = {"p_1": procds_id}
    dbConn.execute(sqlQuery, bindVars)
    result = dbConn.fetchall()

    if len(result) == 1:
        return result[0][0]
    else:
        return None

def listDataTierNameFromID(dbConn, datatier_id):
    """
    _listDataTierNameFromID_

    Given the ID of a data tier return it's name.
    """
    sqlQuery = """SELECT a.NAME FROM data_tier a
                  WHERE a.ID = :p_1"""

    bindVars = {"p_1": datatier_id}
    dbConn.execute(sqlQuery, bindVars)
    result = dbConn.fetchall()

    if len(result) == 1:
        return result[0][0]
    else:
        return None

def listDatasetNamesFromID(dbConn, datasetID):
    """
    _listDatasetNamesFromID_

    Given the ID of a dataset return the human readable names of the primary
    dataset, processed dataset and data tier.
    """
    sqlQuery = """SELECT primary_dataset.NAME, processed_dataset.NAME,
                  data_tier.NAME FROM dataset_path INNER JOIN primary_dataset ON
                  primary_dataset.ID = dataset_path.PRIMARY_DATASET INNER JOIN
                  processed_dataset ON
                  processed_dataset.ID = dataset_path.PROCESSED_DATASET INNER JOIN
                  data_tier ON data_tier.ID = dataset_path.DATA_TIER
                  WHERE dataset_path.ID = :p_1"""

    bindVars = {"p_1": datasetID}
    dbConn.execute(sqlQuery, bindVars)
    result = dbConn.fetchall()

    datasetNames = {"PRIMARY": result[0][0], "PROCESSED": result[0][1],
                    "TIER": result[0][2]}
    return datasetNames

def listDatasetIDsFromID(dbConn, datasetID):
    """
    _listDatasetIDsFromID_

    Given the ID of a dataset return the IDs of the primary dataset,
    processed dataset and data tier.
    """
    sqlQuery = """SELECT primary_dataset, processed_dataset, data_tier
                  FROM dataset_path WHERE dataset_path.ID = :p_1"""

    bindVars = {"p_1": datasetID}
    dbConn.execute(sqlQuery, bindVars)
    result = dbConn.fetchall()

    datasetIDs = {"PRIMARY": result[0][0], "PROCESSED": result[0][1],
                    "TIER": result[0][2]}
    return datasetIDs

def listDatasetNamesForWMBSFile(dbConn, wmbsFileID):
    """
    _listDatasetNamesForWMBSFile_

    Retrieve the names of the primary dataset, processed dataset and data tier
    for a given WMBS file.
    """
    sqlQuery = """SELECT primary_dataset.NAME, processed_dataset.NAME,
                  data_tier.NAME FROM dataset_path INNER JOIN primary_dataset ON
                  primary_dataset.ID = dataset_path.PRIMARY_DATASET INNER JOIN
                  processed_dataset ON
                  processed_dataset.ID = dataset_path.PROCESSED_DATASET INNER JOIN
                  data_tier ON data_tier.ID = dataset_path.DATA_TIER INNER JOIN
                  wmbs_file_dataset_path_assoc ON wmbs_file_dataset_path_assoc.dataset_path_id
                  = dataset_path.id WHERE wmbs_file_dataset_path_assoc.file_id = :p_1"""

    bindVars = {"p_1": wmbsFileID}
    dbConn.execute(sqlQuery, bindVars)
    result = dbConn.fetchall()

    datasetNames = {"PRIMARY": result[0][0], "PROCESSED": result[0][1],
                    "TIER": result[0][2]}
    return datasetNames

def listDatasetIDForWMBSFile(dbConn, wmbsFileID):
    """
    _listDatasetIDForWMBSFile_

    Retrieve the dataset_path id for a given WMBS file.
    """
    sqlQuery = """SELECT id FROM dataset_path INNER JOIN wmbs_file_dataset_path_assoc ON
                  wmbs_file_dataset_path_assoc.dataset_path_id = dataset_path.id
                  WHERE wmbs_file_dataset_path_assoc.file_id = :p_1"""

    bindVars = {"p_1": wmbsFileID}
    dbConn.execute(sqlQuery, bindVars)
    result = dbConn.fetchall()
    return result[0][0]

def listDatasetsForDQMHarvest(dbConn, runNumber):
    """
    _listDatasetsForDQMHarvest_

    Retrieve a list of all the primary/processed dataset combinations that have
    been produced for reconstructed files and a given run.  Results are returned
    in the form of a list of dictionaries where the primary dataset name is
    under the "PRIMARY" key and the processed dataset name is under the
    "PROCESSED" key.
    """
    sql = """SELECT DISTINCT primary_dataset.name, processed_dataset.name FROM dataset_path
             INNER JOIN primary_dataset ON primary_dataset.id = dataset_path.primary_dataset
             INNER JOIN processed_dataset ON processed_dataset.id = dataset_path.processed_dataset
             INNER JOIN wmbs_file_dataset_path_assoc ON wmbs_file_dataset_path_assoc.dataset_path_id = dataset_path.id
             INNER JOIN wmbs_fileset_files ON wmbs_fileset_files.fileid = wmbs_file_dataset_path_assoc.file_id
             INNER JOIN wmbs_file_runlumi_map ON wmbs_file_runlumi_map.fileid = wmbs_file_dataset_path_assoc.file_id
             WHERE run = :p_1 AND dataset_path.data_tier = (SELECT id FROM data_tier WHERE name = 'RECO') AND
             wmbs_fileset_files.fileset = (SELECT id FROM wmbs_fileset WHERE name = 'DBSUploadable')"""

    bindVars = {"p_1": runNumber}
    dbConn.execute(sql, bindVars)
    results = dbConn.fetchall()

    datasets = []
    for result in results:
        newDataset = {"PRIMARY": result[0], "PROCESSED": result[1]}
        datasets.append(newDataset)

    return datasets

def listProcessedDatasetID(dbConn, processedDatasetName):
    """
    _listProcessedDatasetID_

    Retrieve the ID of a processed dataset.
    """
    sqlQuery = "SELECT id FROM processed_dataset WHERE name = :p_1"
    bindVars = {"p_1": processedDatasetName}
    dbConn.execute(sqlQuery, bindVars)
    result = dbConn.fetchall()

    return result[0][0]

def listDataTierID(dbConn, dataTierName):
    """
    _listDataTierID_

    Retrieve the ID of a data tier.
    """
    sqlQuery = "SELECT id FROM data_tier WHERE name = :p_1"
    bindVars = {"p_1": dataTierName}
    dbConn.execute(sqlQuery, bindVars)
    result = dbConn.fetchall()

    return result[0][0]

def listDatasetPathFromIDs(dbConn, primaryDatasetID, processedDatasetID,
                           dataTierID):
    """
    _listDatasetPathFromIDs_

    Retrieve the dataset path ID given a primary dataset, processed dataset
    and data tier ID.
    """
    sqlQuery = """SELECT id FROM dataset_path WHERE primary_dataset = :p_1
                  AND processed_dataset = :p_2 AND data_tier = :p_3"""
    bindVars = {"p_1": primaryDatasetID, "p_2": processedDatasetID,
                "p_3": dataTierID}
    dbConn.execute(sqlQuery, bindVars)
    result = dbConn.fetchall()

    return result[0][0]

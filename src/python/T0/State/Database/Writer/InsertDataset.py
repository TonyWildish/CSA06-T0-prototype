#!/usr/bin/env python
"""
_InsertDataset_

Utilities for inserting and manipulating the dataset tables in T0AST.
"""

__revision__ = "$Id: InsertDataset.py,v 1.10 2009/04/08 20:11:54 sfoulkes Exp $"
__version__ = "$Revision: 1.10 $"

import logging

def insertPrimaryDataset(dbConn, name):
    """
    _insertPrimaryDataset_

    Insert a primary dataset into T0AST.  This will work even if the dataset
    already exists.
    """
    datasetInsert = """INSERT INTO primary_dataset (ID, NAME)
                       SELECT primary_dataset_SEQ.nextval, :p_1 FROM DUAL
                       WHERE NOT EXISTS
                       (SELECT NAME FROM primary_dataset WHERE NAME = :p_1)"""

    bindVars = {"p_1": name}
    dbConn.execute(datasetInsert, bindVars)
    return

def insertProcessedDataset(dbConn, name):
    """
    _insertProcessedDataset_

    Insert a processed dataset into T0AST.  This will work even if the dataset
    already exists.
    """
    datasetInsert = """INSERT INTO processed_dataset (ID, NAME)
                       SELECT processed_dataset_SEQ.nextval, :p_1 FROM DUAL
                       WHERE NOT EXISTS
                       (SELECT NAME FROM processed_dataset WHERE NAME = :p_1)"""

    bindVars = {"p_1": name}
    dbConn.execute(datasetInsert, bindVars)
    return

def insertDataTier(dbConn, name):
    """
    _insertDataTier_

    Insert a data tier into T0AST.  This will work even if the data tier already
    exists.
    """
    datasetInsert = """INSERT INTO data_tier (ID, NAME)
                       SELECT data_tier_SEQ.nextval, :p_1 FROM DUAL
                       WHERE NOT EXISTS
                       (SELECT NAME FROM data_tier WHERE NAME = :p_1)"""

    bindVars = {"p_1": name}
    dbConn.execute(datasetInsert, bindVars)
    return    

def insertDataset(dbConn, primaryDatasetID, processedDatasetID, dataTierID):
    """
    _insertDataset_

    Insert a fully qualified dataset (primary, processed, data tier) into
    T0AST.  This will work even if the dataset already exists.
    """
    sqlQuery = """INSERT INTO dataset_path (id, primary_dataset, processed_dataset,
                  data_tier) SELECT dataset_SEQ.nextval, :p_1, :p_2, :p_3
                  FROM DUAL WHERE NOT EXISTS (SELECT * FROM dataset_path WHERE
                  primary_dataset = :p_1 AND processed_dataset = :p_2 AND
                  data_tier = :p_3)"""

    bindVars = {"p_1": primaryDatasetID, "p_2": processedDatasetID,
                "p_3": dataTierID}
    dbConn.execute(sqlQuery, bindVars)
    return

def insertDatasetByName(dbConn, primaryDatasetName, processedDatasetName, dataTierName):
    """
    _insertDatasetByName_

    Insert a fully qualified dataset (primary, processed, data tier) into
    T0AST.  This will work even if the dataset already exists.
    """
    sqlQuery = """INSERT INTO dataset_path (id, primary_dataset, processed_dataset, data_tier)
                  SELECT dataset_SEQ.nextval,
                         (SELECT id FROM primary_dataset WHERE name = :p_1),
                         (SELECT id FROM processed_dataset WHERE name = :p_2),
                         (SELECT id FROM data_tier WHERE name = :p_3) FROM DUAL
                  WHERE NOT EXISTS
                    (
                      SELECT * FROM dataset_path
                      WHERE primary_dataset = (SELECT id FROM primary_dataset WHERE name = :p_1)
                      AND processed_dataset = (SELECT id FROM processed_dataset WHERE name = :p_2)
                      AND data_tier = (SELECT id FROM data_tier WHERE name = :p_3)
                    )"""

    bindVars = {"p_1": primaryDatasetName,
                "p_2": processedDatasetName, 
                "p_3": dataTierName}
    dbConn.execute(sqlQuery, bindVars)
    return

def assocPrimaryDatasetRunStream(dbConn, primaryDatasetName, streamName,
                                 runNumber):
    """
    _assocPrimaryDatasetRunStream_

    Associate a primary dataset to a run and stream.  This will work even if the
    association has already been setup.
    """
    datasetAssoc = """INSERT INTO dataset_run_stream_assoc (RUN_ID, STREAM_ID,
                      PRIMARY_DATASET_ID) SELECT :p_3, (SELECT ID FROM stream
                      WHERE NAME = :p_1), (SELECT ID FROM primary_dataset
                      WHERE NAME = :p_2) FROM DUAL WHERE NOT
                      EXISTS (SELECT RUN_ID FROM dataset_run_stream_assoc WHERE
                      RUN_ID = :p_3 AND PRIMARY_DATASET_ID = (SELECT ID FROM
                      primary_dataset WHERE NAME = :p_2) AND
                      STREAM_ID = (SELECT ID FROM stream WHERE NAME = :p_1))"""

    bindVars = {"p_1": streamName, "p_2": primaryDatasetName, "p_3": runNumber}
    dbConn.execute(datasetAssoc, bindVars)
    return

def assocWMBSFileDataset(dbConn, primaryDatasetID, processedDatasetID,
                         dataTierID, wmbsFileID):
    """
    _assocWMBSFileDataset_

    Associate a WMBSFile to a particular dataset.  This will create the dataset
    if it does not already exist.
    """
    insertDataset(dbConn, primaryDatasetID, processedDatasetID, dataTierID)

    sqlQuery = """INSERT INTO wmbs_file_dataset_path_assoc (file_id, dataset_path_id)
                  VALUES (:p_1, (SELECT id FROM dataset_path WHERE
                  primary_dataset = :p_2 AND processed_dataset = :p_3 AND
                  data_tier = :p_4))"""

    bindVars = {"p_1": wmbsFileID, "p_2": primaryDatasetID,
                "p_3": processedDatasetID, "p_4": dataTierID}
    dbConn.execute(sqlQuery, bindVars)
    return

def assocWMBSFileDatasetByNames(dbConn, wmbsFileID, primaryDatasetName, 
                                processedDatasetName, dataTierName):
    """
    _assocWMBSFileDataset_

    Associate a WMBSFile to a particular dataset. dataset should be exist already
    """
    sqlQuery = """INSERT INTO wmbs_file_dataset_path_assoc (file_id, dataset_path_id)
                  VALUES (:p_1, 
                          (SELECT id FROM dataset_path WHERE primary_dataset = 
                           (SELECT id FROM primary_dataset WHERE name = :p_2)
                            AND processed_dataset = 
                           (SELECT id FROM processed_dataset WHERE name = :p_3)
                            AND data_tier =
                           (SELECT id FROM data_tier WHERE name = :p_4))
                           )"""

    bindVars = {"p_1": wmbsFileID, "p_2": primaryDatasetName,
                "p_3": processedDatasetName, "p_4": dataTierName}
    dbConn.execute(sqlQuery, bindVars)
    return

def assocExpressFileStreamDataset(dbConn, wmbsFileID, streamName, dataTierName, 
                                 processedDatasetName, primaryDatasetName):
    """
    _assocExpressFileStreamDataset_

    Associate a WMBSFile to a particular dataset. dataset should be exist already
    """
    sqlQuery = """INSERT INTO express_file_info (file_id, stream_id, data_tier, processed_dataset, primary_dataset)
                  VALUES (:p_1,
                          (SELECT id FROM stream WHERE name = :p_2),
                          (SELECT id FROM data_tier WHERE name = :p_3),
                          (SELECT id FROM processed_dataset WHERE name = :p_4),
                          (SELECT id FROM primary_dataset WHERE name = :p_5) 
                          )"""

    bindVars = {"p_1": wmbsFileID,  "p_2": streamName, 
                "p_3": dataTierName, "p_4": processedDatasetName, 
                "p_5": primaryDatasetName}
    
    dbConn.execute(sqlQuery, bindVars)
    return

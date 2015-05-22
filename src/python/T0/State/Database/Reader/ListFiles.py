#!/usr/bin/env python
"""
_ListFiles_

Collection of common read method for repacked and reconstructed table
"""

__revision__ = "$Id: ListFiles.py,v 1.40 2009/07/17 18:17:20 sfoulkes Exp $"
__version__ = "$Revision: 1.40 $"

import logging

from T0.DataStructs.T0ASTFile import T0ASTFile

def listFilesForDQM(dbConn, runNumber, primaryDatasetName):
    """
    _listFilesForDQM_

    Retrieve a list of all the LFNs of all the exportable RECO files from a
    particular run and primary dataset.
    """
    sql = """SELECT distinct lfn FROM wmbs_file_details
               INNER JOIN wmbs_file_runlumi_map ON
                 wmbs_file_details.id = wmbs_file_runlumi_map.fileid
               INNER JOIN wmbs_file_dataset_path_assoc ON
                 wmbs_file_dataset_path_assoc.file_id = wmbs_file_details.id
               INNER JOIN dataset_path ON
                 dataset_path.id = wmbs_file_dataset_path_assoc.dataset_path_id
               INNER JOIN primary_dataset ON
                 dataset_path.primary_dataset = primary_dataset.id
             WHERE
               wmbs_file_details.id IN (SELECT fileid FROM wmbs_fileset_files WHERE fileset =
                 (SELECT id FROM wmbs_fileset WHERE name = 'DBSUploadable'))
             AND run = :p_1
             AND primary_dataset.name = :p_2
             AND dataset_path.data_tier = (SELECT id FROM data_tier WHERE name = 'RECO')"""

    bindVars = {"p_1": runNumber, "p_2": primaryDatasetName}
    dbConn.execute(sql, bindVars)
    results = dbConn.fetchall()

    lfns = []
    for result in results:
        lfns.append(result[0])

    return lfns

def listFilesForMerge(dbConn):
    """
    _listFilesForMerge_

    Query the mergable files from T0AST that have not been acquired, completed
    or failed.  For each file, find the minimum run number and lumi section that
    has been associated with it.  Also, if the file is the result of a split
    operation and belongs to a job group find the job group.
    """
    sql = """SELECT wmbs_file_details.id, wmbs_file_details.events,
                 wmbs_file_details.filesize, wmbs_file_details.first_event,
                 wmbs_file_dataset_path_assoc.dataset_path_id,
                 wmbs_file_details.lfn, run, lumi, jobgroup FROM wmbs_file_details
               INNER JOIN wmbs_fileset_files ON
                 wmbs_file_details.id = wmbs_fileset_files.fileid
               INNER JOIN wmbs_subscription ON
                 wmbs_fileset_files.fileset = wmbs_subscription.fileset
               INNER JOIN wmbs_file_dataset_path_assoc ON
                 wmbs_file_details.id = wmbs_file_dataset_path_assoc.file_id
               INNER JOIN
                 (SELECT fileid, MIN(run) AS run, MIN(lumi) AS lumi FROM
                   wmbs_file_runlumi_map GROUP BY fileid) min_lumi_map ON
                 wmbs_file_details.id = min_lumi_map.fileid
               LEFT OUTER JOIN
                 (SELECT id AS jobgroup, fileid FROM wmbs_jobgroup
                   INNER JOIN wmbs_fileset_files ON
                     wmbs_jobgroup.output = wmbs_fileset_files.fileset) jobgroup_map ON
                 wmbs_file_details.id = jobgroup_map.fileid
               LEFT OUTER JOIN wmbs_sub_files_failed ON
                 wmbs_subscription.id = wmbs_sub_files_failed.subscription AND
                 wmbs_file_details.id = wmbs_sub_files_failed.fileid
               LEFT OUTER JOIN wmbs_sub_files_acquired ON
                 wmbs_subscription.id = wmbs_sub_files_acquired.subscription AND
                 wmbs_file_details.id = wmbs_sub_files_acquired.fileid
               LEFT OUTER JOIN wmbs_sub_files_complete ON
                 wmbs_subscription.id = wmbs_sub_files_complete.subscription AND
                 wmbs_file_details.id = wmbs_sub_files_complete.fileid
               WHERE
                 wmbs_sub_files_failed.fileid IS Null AND
                 wmbs_sub_files_acquired.fileid IS Null AND
                 wmbs_sub_files_complete.fileid IS Null AND
                 wmbs_fileset_files.fileset = (SELECT id FROM wmbs_fileset WHERE name = 'Mergeable')
             """
    
    dbConn.execute(sql)
    results = dbConn.fetchall()

    mergeableFiles = []
    for result in results:
        mergeableFile = {}
        mergeableFile["ID"] = result[0]
        mergeableFile["EVENTS"] = result[1]
        mergeableFile["SIZE"] = result[2]
        mergeableFile["FIRST_EVENT"] = result[3]
        mergeableFile["DATASET"] = result[4]
        mergeableFile["LFN"] = result[5]
        mergeableFile["RUN"] = result[6]
        mergeableFile["LUMI"] = result[7]

        # The sorting algorithm in the Tier0Merger expects the JobGroup ID
        # to be a number.  If the file wasn't part of a split job we'll set
        # the JobGroup ID to be -1 so that the file is sorted correctly.
        if result[8] == None:
            mergeableFile["JOBGROUP"] = -1
        else:
            mergeableFile["JOBGROUP"] = result[8]
            
        mergeableFiles.append(mergeableFile)

    return mergeableFiles

def listFilesForExpressMerge(dbConn):
    """
    _listFilesForExpressMerge_

    Query the mergable files from T0AST that have not been acquired, completed
    or failed.  For each file, find the minimum run number and lumi section that
    has been associated with it.  Also, if the file is the result of a split
    operation and belongs to a job group find the job group.
    """
    sqlQuery = """SELECT a.fileid, e.stream_id, e.data_tier, e.processed_dataset, e.primary_dataset,
                         f.run, f.lumi, d.events, d.filesize, d.lfn, j.insert_time
                  FROM wmbs_fileset_files a
                  INNER JOIN wmbs_fileset b
                  ON b.id = a.fileset
                  INNER JOIN wmbs_file_details d
                  ON d.id = a.fileid
                  INNER JOIN express_file_info e
                  ON e.file_id = a.fileid
                  INNER JOIN wmbs_file_runlumi_map f
                  ON f.fileid = a.fileid
                  LEFT OUTER JOIN wmbs_sub_files_acquired g
                  ON g.fileid = a.fileid
                  LEFT OUTER JOIN wmbs_sub_files_failed h
                  ON h.fileid = a.fileid
                  LEFT OUTER JOIN wmbs_sub_files_complete i
                  ON i.fileid = a.fileid
                  INNER JOIN lumi_section_express_done j
                  ON j.run_id = f.run AND j.lumi_id = f.lumi AND j.stream_id = e.stream_id 
                  WHERE b.name = 'ExpressMergeable'
                  AND g.fileid IS NULL
                  AND h.fileid IS NULL
                  AND i.fileid IS NULL
                  """

    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()

    mergeableFiles = []
    for result in results:
        mergeableFile = {}
        mergeableFile["ID"] = result[0]
        mergeableFile["STREAM_ID"] = result[1]
        mergeableFile["DATATIER_ID"] = result[2]
        mergeableFile["PROCDS_ID"] = result[3]
        mergeableFile["PRIMDS_ID"] = result[4]
        mergeableFile["RUN"] = result[5]
        mergeableFile["LUMI"] = result[6]
        mergeableFile["EVENTS"] = result[7]
        mergeableFile["SIZE"] = result[8]
        mergeableFile["LFN"] = result[9]
        mergeableFile["LUMI_DONE_TIME"] = result[10]

        mergeableFiles.append(mergeableFile)

    return mergeableFiles

def countMergeableFiles(dbConn, runNumber, dataTierName):
    """
    _countMergeableFiles_

    Count the number of files in the Mergeable fileset that belong to a
    particular run and data tier that have not been marked as "Complete"
    or "Failed".
    """
    sql = """SELECT COUNT(DISTINCT wmbs_fileset_files.fileid) FROM wmbs_fileset_files
               INNER JOIN wmbs_file_runlumi_map ON
                 wmbs_fileset_files.fileid = wmbs_file_runlumi_map.fileid
               INNER JOIN wmbs_fileset ON
                 wmbs_fileset_files.fileset = wmbs_fileset.id
               INNER JOIN wmbs_subscription ON
                 wmbs_fileset.id = wmbs_subscription.fileset
               INNER JOIN wmbs_file_dataset_path_assoc ON
                 wmbs_file_dataset_path_assoc.file_id = wmbs_fileset_files.fileid
               INNER JOIN dataset_path ON
                 dataset_path.id = wmbs_file_dataset_path_assoc.dataset_path_id                 
               LEFT OUTER JOIN wmbs_sub_files_failed ON
                 wmbs_subscription.id = wmbs_sub_files_failed.subscription AND
                 wmbs_fileset_files.fileid = wmbs_sub_files_failed.fileid
               LEFT OUTER JOIN wmbs_sub_files_complete ON
                 wmbs_subscription.id = wmbs_sub_files_complete.subscription AND
                 wmbs_fileset_files.fileid = wmbs_sub_files_complete.fileid
               WHERE
                 wmbs_fileset.name = 'Mergeable'
               AND
                 dataset_path.data_tier = (SELECT id FROM data_tier WHERE name = :p_1)
               AND
                 run = :p_2
               AND
                 wmbs_sub_files_complete.fileid is Null
               AND
                 wmbs_sub_files_failed.fileid is Null
               """

    bindVars = {"p_1": dataTierName, "p_2": runNumber}
    dbConn.execute(sql, bindVars)
    return dbConn.fetchall()[0][0]

def listFileIDsByBlockID(dbConn, blockID):
    """
    _listFileIDsByBlockID_
    
    """
    sqlQuery = """SELECT file_id FROM wmbs_file_block_assoc 
                  WHERE block_id = :block_id"""
    bindVars = {"block_id": blockID}
    
    dbConn.execute(sqlQuery, bindVars)
    result = dbConn.fetchall()
    fileIDs = []
    for row in result:
        fileIDs.append(row[0])
    return fileIDs
     
def getRepackHLTDebugAssocFileIDs(dbConn, fileIDs, dataTier):
    """
    _getHLTDebugFileParentage_

    Locate the files in T0AST that are associated with a particular job ID
    and set them to be the inputs of the file that was generated by the job.
    This currently supports the following job types:
      Merge
    """
    if dataTier == "HLTDEBUG":
        inputID = "HLTDEBUG_ID"
        outputID = "REPACKED_ID"
    elif dataTier == "RAW":
        inputID = "REPACKED_ID"
        outputID = "HLTDEBUG_ID"
    else:
        logging.error("Unknown data tier: %s" % dataTier)
        return        
             
    sqlQuery = """ SELECT %s FROM repack_hltdebug_parentage
                      WHERE %s in (""" % (outputID, inputID)
      
    bindVars = {}
    count = 0
    for fileID in fileIDs:
        count += 1
        sqlQuery += ":p_%s, " % count
        bindVars["p_%s" % count] = fileID
    sqlQuery = sqlQuery.rstrip(", ") + ")"
    
    
    # To do: not sure why this is not working
#    sqlQuery = """ SELECT %s FROM repack_hltdebug_parentage
#                      WHERE %s = :p_1 """ % (outputID, inputID)
    
#    bindVars = []
#    for fileID in fileIDs:
#        bindVars.append({'p_1': fileID})
#      
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()
    fileIDList = []
    for row in results:
        fileIDList.append(row[0])
    
    return fileIDList   

def listBlockFilesForSkim(dbConn, blockID):
    """
    _listBlockFilesForSkim_

    Retrieve a list of files in a block for Tier1 skimming.  This query will
    return a list of dictionaries, each dictionary will have the following
    keys:
      EVENTS, SIZE, LFN
    """
    sqlQuery = """SELECT events, filesize, lfn FROM wmbs_file_details
                    INNER JOIN wmbs_file_block_assoc
                      ON wmbs_file_details.id = wmbs_file_block_assoc.file_id
                  WHERE wmbs_file_block_assoc.block_id = :p_1"""
    bindVars = {"p_1": blockID}
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    files = []
    for result in results:
        newFile = {}
        newFile["EVENTS"] = result[0]
        newFile["SIZE"] = result[1]
        newFile["LFN"] = result[2]
        files.append(newFile)

    return files

def listLumiInfoForFile(dbConn, lfn):
    """
    _listLumiInfoForFile_

    Retrieve a list of lumi IDs for a given LFN.
    """
    sqlQuery = """SELECT DISTINCT lumi FROM wmbs_file_runlumi_map
                    INNER JOIN wmbs_file_details
                      ON wmbs_file_details.id = wmbs_file_runlumi_map.fileid
                  WHERE wmbs_file_details.lfn = :p_1"""
    bindVars = {"p_1": lfn}
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    lumiIDs = []
    for result in results:
        lumiIDs.append(result[0])

    return lumiIDs

def listParentLFNs(dbConn, childLFN):
    """
    _listParentLFNs_

    Retrieve the LFNs of a file's parents.  This is used by the Tier1Scheduler
    as it doesn't have a good way of using the WMBS classes with more than
    one database at a time.

    This query will actually retrieve the grand parent LFNs as we don't care
    about intermediate files that get merged.
    """
    sql = """SELECT DISTINCT lfn FROM wmbs_file_details
               INNER JOIN wmbs_file_parent ON wmbs_file_details.id = wmbs_file_parent.parent
             WHERE wmbs_file_parent.child IN
               (SELECT parent FROM wmbs_file_parent
                  WHERE child = (SELECT id FROM wmbs_file_details WHERE lfn = :lfn))"""
        
    bindVars = {"lfn": childLFN}

    dbConn.execute(sql, bindVars)
    results = dbConn.fetchall()

    parentLFNs = []
    for result in results:
        parentLFNs.append(result[0])

    return parentLFNs

def listParentLFNsForExpressFile(dbConn, fileID, parentTierList):
    """
    _listLumiInfoForFile_

    Retrieve a list of lumi IDs for a given LFN.
    parentTierList is normally just one tier but if two file input is allowed,
    could be multiple tier
    """
    sqlQuery = """SELECT DISTINCT wfd.lfn FROM wmbs_file_details wfd 
                    INNER JOIN wmbs_file_runlumi_map wfrm
                      ON wfd.id = wfrm.fileid
                    INNER JOIN (SELECT run, lumi FROM wmbs_file_runlumi_map 
                                WHERE fileid = :p_1)
                      USING (run, lumi)
                    INNER JOIN wmbs_fileset_files wff
                      ON (wfd.id = wff.fileid)
                    INNER JOIN wmbs_fileset wf
                      ON (wff.fileset = wf.id)
                    INNER JOIN wmbs_file_dataset_path_assoc wfdps
                      ON (wfd.id = wfdps.file_id)
                    INNER JOIN dataset_path dp
                      ON (wfdps.dataset_path_id = dp.id)
                    INNER JOIN data_tier dt
                      ON (dt.id = dp.data_tier)
                  WHERE wfd.id != :p_1 and wf.name = 'ExpressDBSUploadable' 
                        and dt.name = :p_2"""
    
    if type(parentTierList) == str:
        parentTierList = [parentTierList]
    
    bindVars = []
    for tier in parentTierList:
        params = {}
        params = {"p_1": fileID, "p_2":tier}
        
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    parentLFNs = []
    for result in results:
        parentLFNs.append(result[0])

    return parentLFNs

def listJobGroupForFileID( dbConn, fileID ):
    sqlQuery = """
               SELECT jg.id
                  FROM wmbs_jobgroup jg
                  INNER JOIN wmbs_fileset_files ff ON jg.output = ff.fileset
                  INNER JOIN wmbs_file_details fd ON fd.id = ff.fileid
                  WHERE fd.id = :fileID
                 """
    bindVars = {"fileID": fileID}
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()
    if len(results) != 1:
        logging.error( "Didn't get exactly one job group returned for fileID: %d" % fileID )

    return results[0][0]

def getTierByFileID(dbConn, fileID):
    """
    _getTierByFileID_
    
    get tier information for the given file 
    """
    
    sqlQuery = """SELECT dt.name FROM data_tier dt
                    INNER JOIN dataset_path dp ON dp.data_tier = dt.id
                    INNER JOIN wmbs_file_dataset_path_assoc fdp ON fdp.dataset_path_id = dp.id
                  WHERE fdp.file_id = :fileID"""
    
    bindVars = {"fileID": fileID}
    
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()
    return results[0][0]

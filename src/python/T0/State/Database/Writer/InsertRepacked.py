#!/usr/bin/env python
"""
_InsertRepacked_

Functions for inserting details about a repacked file.
"""

__revision__ = "$Id: InsertRepacked.py,v 1.25 2009/02/18 13:25:30 hufnagel Exp $"
__version__ = "$Revision: 1.25 $"
import logging

def repackedStreamerAssoc(dbConn, jobID, repackedID):
    """
    _repackedStreamerAssoc_

    Create a link between a repacked file and all the streamer files that
    were used in its creation.
    """
    streamerQuery = """SELECT STREAMER_ID FROM job_streamer_dataset_assoc WHERE
                       JOB_ID = :p_1 and PRIMARY_DATASET_ID = 
                       (SELECT PRIMARY_DATASET FROM dataset_path WHERE ID = 
                          (SELECT dataset_path_id FROM wmbs_file_dataset_path_assoc
                           WHERE FILE_ID = :p_2))"""
    
    bindVars = {"p_1": jobID, "p_2": repackedID}
    dbConn.execute(streamerQuery, bindVars)
    results = dbConn.fetchall()

    sqlQuery = """INSERT INTO repack_streamer_assoc (REPACKED_ID, STREAMER_ID)
                  VALUES (:p_1, :p_2)"""
    rowsToInsert = []
    for result in results:
        dataToInsert = {"p_1": repackedID, "p_2": result[0]}
        rowsToInsert.append(dataToInsert)

    dbConn.execute(sqlQuery, rowsToInsert)
    return

def expressStreamerAssoc(dbConn, jobID, expressID):
    """
    _expressStreamerAssoc_
    
    Create a link between a expressed file and all the streamer files that
    were used in its creation.
    To do: this need to be changed if express process create more than one 
    file. (ie dataset then use something like repacked streamer assoc)
    """
    streamerQuery = """SELECT STREAMER_ID FROM job_streamer_assoc WHERE
                       JOB_ID = :p_1"""
    
    bindVars = {"p_1": jobID}
    dbConn.execute(streamerQuery, bindVars)
    results = dbConn.fetchall()

    sqlQuery = """INSERT INTO repack_streamer_assoc (REPACKED_ID, STREAMER_ID)
                  VALUES (:p_1, :p_2)"""
    rowsToInsert = []
    for result in results:
        dataToInsert = {"p_1": expressID, "p_2": result[0]}
        rowsToInsert.append(dataToInsert)

    dbConn.execute(sqlQuery, rowsToInsert)
    return

# commented out temporay if the performance is the problem in Tier0Accountant Repack hanlder
# use this one instead        
#def repackedLumiAssoc(dbConn, repackedID, runNumber):
#    """
#    _repackedLumiAssoc_
#
#    Create a link between a repacked file and all the lumi sections that
#    were used in its creation.  This function assumes that the associations
#    between the repacked file and the streamers used to create it have already
#    been setup.
#    """
#    lumiQuery = """SELECT DISTINCT streamer.LUMI_ID FROM streamer INNER JOIN
#                   repack_streamer_assoc USING (STREAMER_ID) WHERE
#                   repack_streamer_assoc.REPACKED_ID = :p_1"""
#
#    bindVars = {"p_1": repackedID}
#    dbConn.execute(lumiQuery, bindVars)
#    results = dbConn.fetchall()
#    
#    # better to use DOA AddRunLumi from file  outside the function, then 
#    # if the performance is the problem use this instead
#    sqlQuery = """INSERT INTO wmbs_file_runlumi_map (fileid, run, lumi)
#                  VALUES (:p_1, :p_2, :p_3)"""               
#
#    rowsToInsert = []
#    for result in results:
#        dataToInsert = {"p_1": repackedID, "p_2": runNumber, "p_3": result[0]}
#        rowsToInsert.append(dataToInsert)
#
#    dbConn.execute(sqlQuery, rowsToInsert)
#    return
#
#    

def repackedHltDebugParentage(dbConn, repackedID, hltdebugID):
    """
    _repackedHltDebugParentage_

    Add a row to the repack_hltdebug_parentage table so we can track the
    parentage of hltdebug files.
    """
    sqlQuery = """INSERT INTO repack_hltdebug_parentage (REPACKED_ID,
                  HLTDEBUG_ID) VALUES (:p_1, :p_2)"""
    bindVars = {"p_1": repackedID, "p_2": hltdebugID}

    dbConn.execute(sqlQuery, bindVars)
    return

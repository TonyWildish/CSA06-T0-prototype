#!/usr/bin/env python
"""
_ListTriggerSegments_

Functions to query the trigger_segment table in T0AST.
"""

__revision__ = "$Id: ListTriggerSegments.py,v 1.20 2009/02/18 13:25:30 hufnagel Exp $"
__version__ = "$Revision: 1.20 $"

def listNewTriggerSegments(dbConn, status):
    """
    _listNewTriggerSegments_

    Retrieve a list of trigger_segments from T0AST that have a particular status
    """
    sqlQuery = """SELECT PRIMARY_DATASET_ID, STREAMER_ID, LUMI_ID, RUN_ID, SEGMENT_SIZE
                  FROM trigger_segment
                  WHERE STATUS = (SELECT ID FROM trigger_segment_status WHERE STATUS = :p_1)"""

    bindVars = {"p_1": status}
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()
    
    return results

def listNewTriggerSegmentsByRun(dbConn, runNumber, datasetName, status):
    """
    _listNewTriggerSegmentsByRun_

    Retrieve a list of trigger_segments from T0AST that have a particular
    status and belong to a particular primary dataset and run.
    """
    sqlQuery = """SELECT PRIMARY_DATASET_ID, STREAMER_ID, LUMI_ID, RUN_ID, SEGMENT_SIZE,
                  STATUS FROM trigger_segment WHERE RUN_ID = :p_1 AND
                  PRIMARY_DATASET_ID = (SELECT ID FROM primary_dataset WHERE
                  NAME = :p_2) AND STATUS = (SELECT ID FROM
                  trigger_segment_status WHERE STATUS = :p_3)"""
    
    bindVars = {"p_1": runNumber, "p_2": datasetName, "p_3": status}
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()
    
    return results

def countTriggerSegmentsByStatus(dbConn, runNumber, statusList):
    """
    _countTriggerSegmentsByStatus_

    Count the number of trigger_segments from a particular run with a particular
    status.  Note that each entry in the status list must correspond with a row
    in the trigger_segment_status table.
    """
    if type(statusList) != list:
        statusList = [statusList]
    
    sqlQuery = """SELECT COUNT(*) FROM trigger_segment WHERE RUN_ID = :p_1
                  AND STATUS = (SELECT ID FROM trigger_segment_status WHERE
                  STATUS = :p_2)""" 

    bindVars = []
    for status in statusList:
        bindVars.append({"p_1": runNumber, "p_2": status})

    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    total = 0
    for result in results:
        total += int(result[0])

    return total

def countTriggerSegmentsByRun(dbConn, runNumber):
    """
    _countTriggerSegmentsByRun_

    Count the number of trigger segments that exist in T0AST for a
    particular run.
    """
    sqlQuery = "SELECT COUNT(*) FROM trigger_segment WHERE RUN_ID = :p_1"

    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    result = dbConn.fetchall()

    while type(result) != int:
        result = result[0]

    return result

def getTriggerSegmentByJobID(dbConn, jobID):
    """
    _getTriggerSegmentByJobID_

    Retrieve information about all trigger segments that are associated with
    a particular repack job.
    """
    sqlQuery = """SELECT PRIMARY_DATASET_ID, STREAMER_ID FROM job_streamer_dataset_assoc
                  WHERE JOB_ID = :p_1"""

    bindVars = {"p_1": jobID}
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()
    
    return results

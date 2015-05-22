#!/usr/bin/env python
"""
_InsertTriggerSegment_

Functions for manipulating data in the trigger_segment table.
"""

__revision__ = "$Id: InsertTriggerSegment.py,v 1.4 2009/02/05 14:24:21 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

def insertTriggerSegments(dbConn, triggerSegments):
    """
    _insertTriggerSegments_

    Insert trigger segments into the trigger_segment table.  Note that the
    triggerSegments parameter must be a list of dictionaries.  Each dictionary
    must have the following keys:
      dataset_id
      streamer_id
      lumi_id
      run_id
      segment_size
      status
    """
    if type(triggerSegments) != list:
        triggerSegments = [triggerSegments]
    
    sqlQuery = """INSERT INTO trigger_segment (PRIMARY_DATASET_ID, STREAMER_ID,
                  LUMI_ID, RUN_ID, SEGMENT_SIZE, STATUS) VALUES (:dataset_id,
                  :streamer_id, :lumi_id, :run_id, :segment_size, :status)"""

    dbConn.execute(sqlQuery, triggerSegments)
    return

def updateTriggerSegmentStatus(dbConn, streamerIDs, datasetIDs, status):
    """
    _updateTriggerSegmentStatus_

    Update the status of a trigger segment.  The status parameter must be a
    string that corresponds to a row in the trigger_segment_status table.
    """
    sqlQuery = """UPDATE trigger_segment SET STATUS = (SELECT ID FROM
                  TRIGGER_SEGMENT_STATUS WHERE STATUS = :p_1) WHERE
                  STREAMER_ID = :p_2 AND PRIMARY_DATASET_ID = :p_3"""
    
    for streamerID in streamerIDs:
        for datasetID in datasetIDs:
            bindVars = {"p_1": status, "p_2": streamerID, "p_3": datasetID}
            dbConn.execute(sqlQuery, bindVars)
    return

def deleteTriggerSegments(dbConn, streamerIDs, datasetIDs):
    """
    _deleteTriggerSegments_

    Delete all trigger segments for the provided
    dataset and streamers ids

    """

    sqlQuery = """DELETE from trigger_segment WHERE
                  STREAMER_ID = :p_2 AND PRIMARY_DATASET_ID = :p_3"""

    for streamerID in streamerIDs:
        for datasetID in datasetIDs:
            bindVars = {"p_1": status, "p_2": streamerID, "p_3": datasetID}
            dbConn.execute(sqlQuery, bindVars)
    return

#!/usr/bin/env python
"""
_InsertStreamer_

Functions for updating information in the streamer table.
"""

__revision__ = "$Id: InsertStreamer.py,v 1.15 2009/07/06 12:02:17 hufnagel Exp $"
__version__ = "$Revision: 1.15 $"

import time
import logging

def updateSplitStreamers(dbConn, streamerIDs):
    """
    _updateSplitStreamers_

    Update the splitable column in the streamer table to be 0 for streamer
    that have been split into trigger segments.  The streamerIDs parameter
    must be a list of streamer ids
    """
    if len(streamerIDs) == 0:
        return
    
    sqlQuery = """INSERT INTO streamer_used (STREAMER_ID, INSERT_TIME) VALUES(:p_1, :p_2)"""

    params = []
    currentTime = int(time.time())
    for streamerID in streamerIDs:
        params.append( {'p_1': streamerID, 'p_2' : currentTime} )
    dbConn.execute(sqlQuery, params)

    return

def insertStreamers(dbConn, streamerList):
    """
    _insertStreamers_
    
    This function is used by Tier0Accountant to insert data in streamer table
    when MC Tier0 Feeder is enabled. In normal operation the data comes from 
    storage manager
    """
    if type(streamerList) != list:
        streamerList = [streamerList]
    
    sqlQuery = """INSERT INTO streamer (streamer_id, run_id, lumi_id, 
                        insert_time, filesize, events, lfn, exportable,
                        stream_id, indexpfn, indexpfnbackup)
                  VALUES (streamer_SEQ.nextval, :run_id, :lumi_id,
                          :insert_time, :filesize, :events, :lfn, :exportable,
                          (SELECT id FROM  stream WHERE name = :stream),
                          :indexpfn, :indexpfnbackup)"""

    bindVars = []
    currentTime = int(time.time())
    for streamer in streamerList:
        bindVar = {}
        bindVar['run_id'] = streamer['RunNumber']
        bindVar['lumi_id'] = streamer['LumiID']
        bindVar['filesize'] = streamer['FileSize']
        bindVar['events'] = streamer['Events']
        bindVar['lfn'] = streamer['LFN']
        bindVar['exportable'] = streamer['Exportable']
        bindVar['stream'] = streamer['Stream']
        bindVar['indexpfn'] = streamer['IndexPFN']
        bindVar['indexpfnbackup'] = streamer['IndexPFNBackup']
        bindVar['insert_time'] = int(time.time())
        bindVars.append(bindVar)

    dbConn.execute(sqlQuery, bindVars)
    
    return

def updateStreamerStatus(dbConn, streamerIDs, status):
    """
    _updateStreamerStatus_

    Update the splitable column in the streamer table to be 0 for streamer
    that have been split into trigger segments.  The streamerIDs parameter
    must be a list of streamer ids
    """
    if len(streamerIDs) == 0:
        return
    
    
    if status.title() == "Deletable":
        statusTable = "streamer_deletable"
    elif status.title() == "Deleted":
        statusTable = "streamer_deleted"
        deleteSql = "DELETE FROM streamer_deletable WHERE streamer_id=:p_1"    
    
    #TODO: check allowing duplicate insert is really safe. Make sure all the 
    # repacked files are created from the given stream file before it marked 
    # for deletion 
    insertSql = """INSERT INTO %s (STREAMER_ID, INSERT_TIME)
                   SELECT :p_1, :p_2 FROM DUAL WHERE NOT EXISTS 
                   (SELECT * FROM %s WHERE STREAMER_ID = :p_1) 
                """ % (statusTable, statusTable) 

    insertParams = []
    deleteParams = []
    currentTime = int(time.time())
    for streamerID in streamerIDs:
        deleteParams.append({'p_1': streamerID})
        insertParams.append({'p_1': streamerID, 'p_2' : currentTime})
    
    if status.title() == "Deleted":
        dbConn.execute(deleteSql, deleteParams)
    
    dbConn.execute(insertSql, insertParams)
    
    return 

#!/usr/bin/env python
"""
_ListStreamers_

Functions for retreiving information from the streamer table in T0AST.
"""

__version__ = "$Revision: 1.21 $"
__revision__ = "$Id: ListStreamers.py,v 1.21 2009/07/17 23:37:56 hufnagel Exp $"

import logging
from T0.DataStructs.StreamerFile import StreamerFile

def getRunsForUnusedStreamers(dbConn):
    """
    _getRunsForUnusedStreamers_

    returns list of runs with unused streamers

    """
    sqlQuery = """SELECT a.RUN_ID FROM streamer a
                  LEFT OUTER JOIN streamer_used b
                  ON b.STREAMER_ID = a.STREAMER_ID
                  WHERE b.STREAMER_ID IS NULL
                  GROUP BY a.RUN_ID"""

    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()

    runs = []
    for result in results:
        runs.append(result[0])

    return runs

def getAvailableExpressStreamers(dbConn):
    """
    _getAvailableExpressStreamers_

    returns all unused streamers of type express

    """
    sqlQuery = """SELECT a.STREAMER_ID, a.RUN_ID, a.LUMI_ID, a.INSERT_TIME,
                  a.EVENTS, a.LFN, a.STREAM_ID FROM streamer a
                  LEFT OUTER JOIN streamer_used b
                  ON b.STREAMER_ID = a.STREAMER_ID
                  INNER JOIN run_stream_style_assoc c
                  ON c.run_id = a.run_id AND c.stream_id = a.stream_id
                  INNER JOIN processing_style d
                  ON d.id = c.style_id
                  WHERE b.STREAMER_ID IS NULL
                  AND d.name = 'Express'"""

    dbConn.execute(sqlQuery)
    streamerRows = dbConn.fetchall()

    streamers = []
    for streamerRow in streamerRows:
        streamerDict = {}
        streamerDict['STREAMER_ID'] = streamerRow[0]
        streamerDict['RUN_ID'] = streamerRow[1]
        streamerDict['LUMI_ID'] = streamerRow[2]
        streamerDict['INSERT_TIME'] = streamerRow[3]
        streamerDict['EVENTS'] = streamerRow[4]
        streamerDict['LFN'] = streamerRow[5]
        streamerDict['STREAM_ID'] = streamerRow[6]
        streamers.append(streamerDict)

    return streamers

def listStreamerIDsForRepackedFiles(dbConn, fileIDs):
    """
    _listStreamersForRepackedFiles_

    Given a list of repacked fileIDs, return a list of the streamer IDs 
    that they were created from.
    """
    if type(fileIDs) != list:
        fileIDs = [fileIDs]

    #if t0astFileList[0]["DATA_TIER"] != "RAW":
    #    return None
    
    sqlQuery = """SELECT streamer_id FROM streamer INNER JOIN
                  repack_streamer_assoc USING(STREAMER_ID) WHERE
                  repack_streamer_assoc.REPACKED_ID = :p_1""" 

    bindVars = []
    for fileID in fileIDs:
        bindVars.append({"p_1": fileID})

    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    streamerIDs = []    
    for result in results:
        streamerIDs.append(result[0])
        
    return streamerIDs

def listStreamersByDeleteStatus(dbConn, status):
    """
    _listStreamersForRepackedFiles_

    Given a delete status, return a list of the Streamers 
    with ID, LFN, IndexLFN and IndexBackUpLFNs field filled
    """
    if status.title() == "Deletable":
        statusTable = "streamer_deletable"
    elif status.title() == "Deleted":
        statusTable = "streamer_deleted"
        
    sqlQuery = """SELECT streamer_id, LFN, INDEXPFN, INDEXPFNBACKUP FROM streamer
                  INNER JOIN %s USING (streamer_id)""" % statusTable 

    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()

    streamers = []    
    for result in results:
        streamer = StreamerFile()
        streamer["ID"] = result[0]
        streamer["LFN"] = result[1]
        streamer["IndexLFN"] = result[2]
        streamer["IndexBackUpLFN"] = result[3]
        streamers.append(streamer)
        
    return streamers

def listStreamersByDeleteStatusAndRun(dbConn, status, runNumber):
    """
    _listStreamersForRepackedFilesAndRun_

    Given a delete status and a runNumber, return a list of the Streamers 
    with ID, LFN, IndexLFN and IndexBackUpLFNs field filled
    """
    if status.title() == "Deletable":
        statusTable = "streamer_deletable"
    elif status.title() == "Deleted":
        statusTable = "streamer_deleted"
        
    sqlQuery = """SELECT streamer_id, LFN, INDEXPFN, INDEXPFNBACKUP FROM streamer
                  INNER JOIN %s USING (streamer_id) WHERE run_id = :runID""" % statusTable 

    dbConn.execute(sqlQuery, {"runID":runNumber})
    results = dbConn.fetchall()

    streamers = []    
    for result in results:
        streamer = StreamerFile()
        streamer["ID"] = result[0]
        streamer["LFN"] = result[1]
        streamer["IndexLFN"] = result[2]
        streamer["IndexBackUpLFN"] = result[3]
        streamers.append(streamer)
        
    return streamers

def countSplitableStreamersByRun(dbConn, runNumber):
    """
    _countSplitableStreamersByRun_

    Count the number of streamer files for a particular run that are marked
    as splitable.
    """
    sqlQuery = """SELECT COUNT(*) FROM streamer a
                  LEFT OUTER JOIN streamer_used b
                  ON a.streamer_id = b.streamer_id
                  INNER JOIN run_stream_style_assoc c
                  ON c.run_id = a.run_id AND c.stream_id = a.stream_id
                  INNER JOIN processing_style d
                  ON d.id = c.style_id
                  WHERE b.streamer_id IS NULL
                  AND a.run_id = :p_1
                  AND d.name = 'Bulk'"""

    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()
    
    total = 0
    for result in results:
        total += int(result[0])
        
    return total

def listSplitableStreamers(dbConn):
    """
    _listSplitableStreamers_

    Retrieve a list of dictionaries containing rows from the streamer table
    that have the splitable column equal to one.
    """
    sqlQuery = """SELECT a.STREAMER_ID, a.RUN_ID, a.LUMI_ID, a.EVENTS, a.LFN, a.INDEXPFN,
                  a.INDEXPFNBACKUP, a.STREAM_ID FROM streamer a
                  INNER JOIN lumi_section_closed b
                  ON b.RUN_ID = a.RUN_ID AND b.lumi_id = a.LUMI_ID AND b.stream_id = a.STREAM_ID
                  LEFT OUTER JOIN streamer_used c
                  ON c.STREAMER_ID = a.STREAMER_ID
                  INNER JOIN run_stream_style_assoc d
                  ON d.run_id = a.run_id AND d.stream_id = a.stream_id
                  INNER JOIN processing_style e
                  ON e.id = d.style_id
                  WHERE c.STREAMER_ID IS NULL
                  AND e.name = 'Bulk'"""

    dbConn.execute(sqlQuery)
    streamerRows = dbConn.fetchall()

    streamers = []
    for streamerRow in streamerRows:
        streamerDict = {}
        streamerDict["STREAMER_ID"] = streamerRow[0]
        streamerDict["RUN_ID"] = streamerRow[1]
        streamerDict["LUMI_ID"] = streamerRow[2]
        streamerDict["EVENTS"] = streamerRow[3]
        streamerDict["LFN"] = streamerRow[4]
        streamerDict["INDEXPFN"] = streamerRow[5]
        streamerDict["INDEXPFNBACKUP"] = streamerRow[6]
        streamerDict["STREAM_ID"] = streamerRow[7]
        streamers.append(streamerDict)

    return streamers

#
# method is only used by the ignoreStreamer script
#
def listSplitableStreamersByRun(dbConn, runNumber):
    """
    _listSplitableStreamersByRun_

    Retrieve a list of dictionaries containing rows from the streamer table
    that have the splitable column equal to one and belong to a specific run.
    """
    sqlQuery = """SELECT a.streamer_id, a.events, a.lfn FROM streamer a
                  LEFT OUTER JOIN streamer_used b
                  ON b.streamer_id = a.streamer_id
                  INNER JOIN run_stream_style_assoc c
                  ON c.run_id = a.run_id AND c.stream_id = a.stream_id
                  INNER JOIN processing_style d
                  ON d.id = c.style_id
                  WHERE b.streamer_id IS NULL
                  AND a.run_id = :p_1
                  AND d.name = 'Bulk'"""
    bindVars = {"p_1": runNumber}

    dbConn.execute(sqlQuery, bindVars)
    streamerRows = dbConn.fetchall()

    streamers = []
    for streamerRow in streamerRows:
        streamerDict = {}
        streamerDict["STREAMER_ID"] = streamerRow[0]
        streamerDict["EVENTS"] = streamerRow[1]
        streamerDict["LFN"] = streamerRow[2]
        streamers.append(streamerDict)

    return streamers


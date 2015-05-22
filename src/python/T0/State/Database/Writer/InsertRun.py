#!/usr/bin/env python
"""
_InsertRun_

Utilities to manipulate the run table in T0AST.
"""

__revision__ = "$Id: InsertRun.py,v 1.20 2009/07/17 23:21:58 hufnagel Exp $"
__version__ = "$Revision: 1.20 $"

import logging
import time

from T0.State.Database.Writer import InsertRunConfig



def insertLumiSection(dbConn, runNumber, lumiSection):
    """
    _insertLumiSection_

    insert run/lumi pair
    """
    sqlQuery = """INSERT INTO lumi_section
                  (lumi_id,run_id,start_time)
                  VALUES (:lumi_id,:run_id,:start_time)"""

    bindVars = { 'run_id' : runNumber,
                 'lumi_id' : lumiSection,
                 'start_time' : 0 }
    dbConn.execute(sqlQuery, bindVars)
    return


def insertRun(dbConn, runNumber, **params):
    """
    _insertRun_

    insert new run record
    called from
      - Tier0MCFeeder injection script
      - Tier0SMFeeder component
    """
    if not params.has_key('hltkey') or \
       not params.has_key('runversion') or \
       not params.has_key('repackversion') or \
       not params.has_key('expressversion'):
        logging.error("ERROR: Cannot insert run, requiered parameters missing")
        return

    InsertRunConfig.insertCMSSWVersion(dbConn, params['runversion'])
    InsertRunConfig.insertCMSSWVersion(dbConn, params['repackversion'])
    InsertRunConfig.insertCMSSWVersion(dbConn, params['expressversion'])

    sqlQuery = """INSERT INTO run
                  (run_id,hltkey,run_version,repack_version,express_version,start_time,end_time,run_status)
                  VALUES (:run_id,:hltkey,
                          (SELECT ID FROM cmssw_version WHERE name = :runversion),
                          (SELECT ID FROM cmssw_version WHERE name = :repackversion),
                          (SELECT ID FROM cmssw_version WHERE name = :expressversion),
                          :starttime,:endtime,
                          (SELECT ID FROM run_status WHERE status = 'Active'))"""

    bindVars = { 'run_id' : runNumber,
                 'hltkey' : params['hltkey'],
                 'runversion' : params['runversion'],
                 'repackversion' : params['repackversion'],
                 'expressversion' : params['expressversion'],
                 'starttime' : 0,
                 'endtime' : 0 }
    dbConn.execute(sqlQuery, bindVars)
    return

def updateRunStatus(dbConn, runNumber, status):
    """
    _updateRunStatus_

    Update the status of a run.  The status parameters must be a string that
    corresponds to a row in the "run_status" table.
    """
    sqlQuery = """UPDATE run SET RUN_STATUS = (SELECT ID FROM run_status WHERE
                  STATUS = :p_1), last_updated = :ctime WHERE RUN_ID = :p_2"""

    bindVars = {"p_1": status, "p_2": runNumber, "ctime": int(time.time())}
    dbConn.execute(sqlQuery, bindVars)
    return

def updateRunStatusAndStreamer(dbConn, runNumber, status, lastStreamer):
    """
    _updateRunStatusAndStreamer_

    Update the status and last_streamer of a run.  The status parameters must
    be a string that corresponds to a row in the "run_status" table.
    """
    sqlQuery = """UPDATE run SET RUN_STATUS = (SELECT ID FROM run_status
                  WHERE STATUS = :p_1), LAST_STREAMER = :p_2 WHERE
                  RUN_ID = :p_3""" 

    bindVars = {"p_1": status, "p_2": lastStreamer, "p_3": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    return

def updateRecoStatusForRun(dbConn, runNumber):
    """
    _updateRecoStatusForRun_

    Update the "RECO_STARTED" column for a run to signify that all PromptReco
    jobs for that run have been released.
    """
    logging.debug("Releasing RECO for run %s" % runNumber)
    sqlQuery = "UPDATE run SET RECO_STARTED = 1 WHERE RUN_ID = :p_1"

    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    return

def releaseRunsForReco(dbConn, timeout):
    """
    _releaseRunsForReco_

    Update the reco_started column for all runs where the
    oldest streamer was inserted at least timeout seconds ago
    """
    sqlQuery = """SELECT a.run_id FROM streamer a
                  INNER JOIN run_stream_style_assoc b
                  ON b.run_id = a.run_id AND b.stream_id = a.stream_id
                  INNER JOIN processing_style c
                  ON c.id = b.style_id
                  INNER JOIN run d
                  ON d.run_id = a.run_id
                  WHERE d.reco_started = 0
                  AND a.insert_time < :p_1
                  AND c.name = 'Bulk'
                  GROUP BY a.run_id"""

    bindVars = { "p_1": time.time() - timeout }
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    runs = []
    for result in results:
        runs.append(result[0])

    if len(runs) > 0:

        sqlQuery = """UPDATE run SET reco_started = 1
        WHERE run_id = :run_id"""

        bindVars = []
        for run in runs:
            bindVars.append({ "run_id": run })
        dbConn.execute(sqlQuery, bindVars)

    return runs

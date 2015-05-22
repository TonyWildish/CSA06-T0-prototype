#!/usr/bin/env python
"""
_ListRuns_

Utilities to retrieve information from the run table in T0AST.
"""

__revision__ = "$Id: ListRuns.py,v 1.21 2009/03/10 13:01:30 sfoulkes Exp $"
__version__ = "$Revision: 1.21 $"

def listRunsByStatus(dbConn, statusList):
    """
    _listRunsByStatus_

    Retrieve a list of dictionaries with information about runs that have a
    particular status.  The statusList parameter can be a string or a list
    of status values.  Note that each item in the statusList list must
    correspond to a row in the run_status table.

    Each dictionary returned will have the following keys:
      RUN_ID - The run number
      RUN_STATUS - The status string for the run
    """
    if type(statusList) != list:
        statusList = [statusList]

    sqlQuery = """SELECT run.RUN_ID, run_status.STATUS FROM run INNER JOIN
                  run_status ON run.RUN_STATUS = run_status.ID WHERE
                  run.RUN_STATUS = (SELECT ID FROM run_status WHERE
                  STATUS = :p_1)"""

    bindVars = []
    for status in statusList:
        bindVars.append({"p_1": status})

    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    runs = []
    for result in results:
        run = {"RUN_ID": result[0], "RUN_STATUS": result[1]}
        runs.append(run)

    return runs

def listRunsByAge(dbConn, maxAge):
    """
    _listRunsByAge_

    Return a list of dictionaries containing information about all of the runs
    with an "Active" status that are at least a certain age.  A runs age is
    defined to be the newest trigger segment that was inserted into T0AST.
    The maxAge in a timestamp in seconds counted from the unix epoch.

    Each dictionary will have the following keys:
      RUN_ID - The run number
      LAST_STREAMER - The ID of the last streamer that was inserted
    """
    sqlQuery = """SELECT RUN_ID, MAX_STREAMER_ID FROM run INNER JOIN (SELECT
                  RUN_ID, MAX(STREAMER_ID) AS MAX_STREAMER_ID, MAX(INSERT_TIME)
                  AS MAX_INSERT_TIME FROM streamer GROUP BY RUN_ID) streamer
                  USING (RUN_ID) WHERE run.RUN_STATUS = (SELECT ID FROM
                  run_status WHERE STATUS = 'Active') AND
                  MAX_INSERT_TIME < :p_1"""
    
    bindVars = {"p_1": maxAge}
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    oldRuns = []
    for result in results:
        oldRun = {"RUN_ID": result[0], "LAST_STREAMER": result[1]}
        oldRuns.append(oldRun)

    return oldRuns

def listRunsWithNewData(dbConn):
    """
    _listRunsWithNewData_

    Return a list of dictionaries containing information about runs that have
    had new streamers show up since they started the close out process.
    """
    sqlQuery = """SELECT RUN_ID, MAX(streamer.STREAMER_ID) FROM run INNER JOIN
                  streamer USING (RUN_ID) WHERE streamer.STREAMER_ID >
                  run.LAST_STREAMER AND run.RUN_STATUS != (SELECT ID FROM
                  run_status WHERE STATUS = 'Active') GROUP BY RUN_ID"""

    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()

    updatedRuns = []
    for result in results:
        updatedRun = {"RUN_ID": result[0], "LAST_STREAMER": result[1]}
        updatedRuns.append(updatedRun)

    return updatedRuns

def listRecoStartedForRun(dbConn, runNumber):
    """
    _listRecoStartedForRun_

    Determine whether or not reconstruction jobs have been released to
    the farm for a particular run.  This will return True if jobs have bene
    released, False otherwise.
    """
    sqlQuery = "SELECT reco_started FROM run WHERE RUN_ID = :p_1"

    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)
    result = dbConn.fetchall()

    if result[0][0] == 1:
        return True

    return False

def listRunState(dbConn, runNumbers):
    """
    _listRunState_

    Retrieve the state of a run for a given run number.
    """
    if type(runNumbers) != list:
        runNumbers = [runNumbers]

    sqlQuery = """SELECT run.run_id, status FROM run_status INNER JOIN run ON
                  run_status.id = run.run_status WHERE run.run_id = :p_1"""

    bindVars = []
    for runNumber in runNumbers:
        bindVar = {"p_1": runNumber}
        bindVars.append(bindVar)
        
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    runStatus = {}
    for result in results:
        runStatus[result[0]] = result[1]

    return runStatus

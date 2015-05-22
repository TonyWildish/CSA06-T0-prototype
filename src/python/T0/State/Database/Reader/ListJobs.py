#!/usr/bin/env python
"""
_ListJobs_

Utilities to retrieve information from the jobs tables in T0AST.
"""

__revision__ = "$Id: ListJobs.py,v 1.22 2009/02/23 15:44:04 hufnagel Exp $"
__version__ = "$Revision: 1.22 $"

import logging


def getNextJobID(dbConn, jobType):
    """
    _getNextJobID_

    gets next free job id depening on job type

    """
    if jobType == "Repack":
        tableName = "repack_job_def"
    elif jobType == "Express":
        tableName = "express_job_def"
    else:
        logging.error("InsertJobDef: Unknown job type: %s" % jobType)
        return None

    idQuery = "SELECT %s_SEQ.nextval from dual" % tableName
    dbConn.execute(idQuery)
    jobID = dbConn.fetchall()

    while type(jobID) != int:
        jobID = jobID[0]

    return jobID

def countRepackJobsByStatus(dbConn, runNumber, statusList):
    """
    _countRepackJobsByStatus_

    Count the number of repacked files that have a certain status.  Every entry
    in the statusList parameter must correspond to a row in the repacked_status
    table.
    """
    if type(statusList) != list:
        statusList = [statusList]
    
    sqlQuery = """SELECT COUNT(DISTINCT JOB_ID) FROM repack_job_def INNER JOIN
                  job_streamer_dataset_assoc USING (JOB_ID) INNER JOIN streamer
                  USING (STREAMER_ID) WHERE streamer.RUN_ID = :p_1 AND
                  repack_job_def.JOB_STATUS = (SELECT ID FROM job_status
                  WHERE STATUS = :p_2)"""

    bindVars = []
    for status in statusList:
        bindVars.append({"p_1": runNumber, "p_2": status})

    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    total = 0
    for result in results:
        total += int(result[0])
                    
    return total

def listJobStatusByID(dbConn, jobType, jobID):
    """
    _listJobStatusByID_

    Return a string containing the status of a job.  This supports the
    following job types:
      Repack
      some other job type maybe need to be added (express stream)

    If the jobID is not in T0AST None is returned.
    """
    if jobType == "Repack":
        jobTable = "repack_job_def"
    elif jobType == "Express":
        jobTable = "express_job_def"
    else:
        logging.error("listJobStatusByID: Unknown Job types %s" % jobType)
        return None

    sqlQuery = """SELECT job_status.STATUS FROM %s INNER JOIN job_status ON
                  job_status.ID = %s.JOB_STATUS WHERE %s.JOB_ID = :p_1""" % \
                  (jobTable, jobTable, jobTable)

    bindVars = {"p_1": jobID}
    dbConn.execute(sqlQuery, bindVars)
    result = dbConn.fetchall()

    if len(result) == 0:
        return None

    while type(result) != str:
        result = result[0]

    return result

def listJobIDByStatus(dbConn, jobType, jobStatus):
    """
    _listJobIDByStatus_

    Return a list of jobs that have a particular status.  The jobType parameter
    must be one of the following:
      Repack
    """
    if jobType == "Repack":
        jobTable = "repack_job_def"
    elif jobType == "Express":
        jobTable = "express_job_def"
    else:
        logging.error("listJobIDByStatus: Unknown Job types %s" % jobType)
        return None

    sqlQuery = """SELECT JOB_ID FROM %s WHERE JOB_STATUS = (SELECT ID from
                  JOB_STATUS where STATUS = :p_1)""" % jobTable
                    
    bindVars = {"p_1": jobStatus}
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    jobIDs = []
    for result in results:
        jobIDs.append(result[0])

    return jobIDs


def listNewRepackJobs(dbConn):
    """
    _listNewRepackJobs_

    Special method to list all new repack jobs
    Includes lumi_id, stream_id and lfn (needed for job creation)

    """
    sqlQuery = """SELECT a.JOB_ID, c.RUN_ID, c.LUMI_ID, c.STREAM_ID, c.LFN FROM repack_job_def a
                  INNER JOIN job_streamer_dataset_assoc b
                  ON a.JOB_ID = b.JOB_ID
                  INNER JOIN streamer c
                  ON b.streamer_id = c.streamer_id
                  WHERE a.JOB_STATUS = (SELECT ID from JOB_STATUS where STATUS = 'New')
                  ORDER by a.JOB_ID"""

    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()

    jobs = []
    for result in results:
        jobDict = {}
        jobDict['JOB_ID'] = result[0]
        jobDict['RUN_ID'] = result[1]
        jobDict['LUMI_ID'] = result[2]
        jobDict['STREAM_ID'] = result[3]
        jobDict['LFN'] = result[4]
        jobs.append(jobDict)

    return jobs


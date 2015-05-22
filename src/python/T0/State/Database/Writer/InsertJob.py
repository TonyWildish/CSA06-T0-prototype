#!/usr/bin/env python
"""
_InsertJob_

Utils for inserting rows into the job_instance and job_def tables as well
as updating the information in them.
"""

__revision__ = "$Id: InsertJob.py,v 1.30 2009/02/18 14:34:48 hufnagel Exp $"
__version__ = "$Revision: 1.30 $"

import time
import logging

from T0.State.Database.Reader.ListJobs import getNextJobID


def insertJobDef(dbConn, jobType, jobStatus, jobID = None):
    """
    _insertJobDef_

    Insert a row into the appropriate job_def table and return the job ID that
    was created.  This function takes two parameters, a reference to an instance
    of the database operations class and a status for the job.  The status is
    a string that must correspond with a status row in the job_status table.
    This function supports the following job types:
      Repack
      ExpressProcess
    """
    if jobType == "Repack":
        tableName = "repack_job_def"
    elif jobType == "Express":
        tableName = "express_job_def"
    else:
        logging.error("InsertJobDef: Unknown job type: %s" % jobType)
        return

    if jobID == None:
        jobID = getNextJobID(dbConn, jobType)

    sqlQuery = """INSERT INTO %s (JOB_ID, JOB_STATUS, DEFINITION_TIME) VALUES
                  (:p_1, (SELECT ID FROM job_status WHERE STATUS = :p_2),
                  :p_3)""" % tableName
                  
    bindVars = {"p_1": jobID, "p_2": jobStatus, "p_3": int(time.time())}
    dbConn.execute(sqlQuery, bindVars)

    return jobID

def updateJobDef(dbConn, jobType, jobID, jobStatus):
    """
    _updateJobDef_

    Update the status of a job def that corresponds to a particular job ID.
    The status is a string that must correspond with a row in the job_status
    table.  This function currently supports the following job types:
      Repack

    If the jobStatus is either "Success", "Failure" or "Abandoned" the
    completion_time column will be updated as well.
    """
    logging.debug("Updating status of %s job %s to %s" % \
                  (jobType, jobID, jobStatus))

    if jobType == "Repack":
        tableName = "repack_job_def"
    elif jobType == "Express":
        tableName = "express_job_def"
    else:
        logging.error("UpdateJobDef: Unknown job type: %s" % jobType)
        return

    bindVars = {"p_1": jobStatus, "p_2": jobID}
    
    if jobStatus in ["Success", "Failure", "Abandoned"]:
        sqlQuery = """UPDATE %s SET JOB_STATUS = (SELECT ID FROM job_status
                      WHERE STATUS = :p_1), COMPLETION_TIME = :p_3
                      WHERE JOB_ID = :p_2""" % tableName
        bindVars["p_3"] = time.time()
    else:
        sqlQuery = """UPDATE %s SET JOB_STATUS = (SELECT ID FROM job_status
                      WHERE STATUS = :p_1) WHERE JOB_ID = :p_2""" % tableName
                  
    dbConn.execute(sqlQuery, bindVars)
    return

def incrementJobRetryCount(dbConn, jobType, jobID):
    """
    _incrementJobRetryCount_

    Increment the "retry_count" field in the job definition table.
    """
    if jobType == "Repack":
        tableName = "repack_job_def"
    elif jobType == "Express":
        tableName = "express_job_def"
    else:
        logging.error("incrementJobRetryCount: Unknown job type: %s" % jobType)
        return

    sqlQuery = """UPDATE %s SET RETRY_COUNT = (SELECT (SELECT RETRY_COUNT FROM
                  %s WHERE JOB_ID = :p_1) + 1 FROM DUAL) WHERE
                  JOB_ID = :p_1""" % (tableName, tableName)
    bindVars = {"p_1": jobID}

    dbConn.execute(sqlQuery, bindVars)
    return


def assocJobToStreamerAndDataset(dbConn, jobID, streamers, datasets):
    """
    _assocRepackJobToDatasetAndStreamer_

    Associate a repack job with the streamers it uses as input and the primary
    dataset it will output.
    """
    assocList = []
##    for segment in segments:
##        assocList.append( { 'job_id' : jobID,
##                            'streamer_id' : segment['StreamerID'],
##                            'primary_dataset_id' : segment['DatasetID'] } )
    for streamerID in streamers:
        for datasetID in datasets:
            assocList.append( { 'job_id' : jobID,
                                'streamer_id' : streamerID,
                                'primary_dataset_id' : datasetID } )

    sqlQuery = """INSERT INTO job_streamer_dataset_assoc
                  (JOB_ID, STREAMER_ID, PRIMARY_DATASET_ID)
                  values (:job_id, :streamer_id, :primary_dataset_id)"""

    dbConn.execute(sqlQuery, assocList)

    return


def assocExpressJobToStreamer(dbConn, jobID, streamers):
    """
    _assocExpressJobToDatasetAndStreamer_

    Associate an express processing job with the streamers it uses as input
    and the primary dataset it will output (the later is optional).
    """
    assocList = []
    for streamerID in streamers:
        assocList.append( { 'job_id' : jobID,
                            'streamer_id' : streamerID } )

    sqlQuery = """INSERT INTO job_streamer_assoc
                  (JOB_ID, STREAMER_ID)
                  values (:job_id, :streamer_id)"""

    dbConn.execute(sqlQuery, assocList)

    return

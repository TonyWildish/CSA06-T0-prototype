"""
API interface specific to RepackerInjector
"""

__revision__ = "$Id: RepackerInjectorAPI.py,v 1.44 2009/02/23 15:23:30 hufnagel Exp $"
__version__ = "$Revision: 1.44 $"

import logging

from T0.DataStructs.StreamerJobEntity import StreamerJobEntity

from T0.State.Database.Reader.ListJobs import listNewRepackJobs
from T0.State.Database.Writer.InsertJob import updateJobDef

from T0.State.Database.Reader import ListDatasets


def getJobs(dbConn):
    """
    _getJobs_

    Retrieve all RepackerScheduler defined jobs from T0AST
    The list I get back contains duplicates (one entry per
    input file per job), so I have to process it out and then
    create and return a list of StreamerJobEntity objects

    STREAM_ID and RUN_ID are identical for each input file
    for a given job (enforced in scheduling rules)

    """
    jobs = listNewRepackJobs(dbConn)

    jobEntityDict = {}
    for job in jobs:
        jobEntity = jobEntityDict.get(job['JOB_ID'], None)
        if jobEntity == None:
            jobEntity = StreamerJobEntity(
                jobID = job['JOB_ID'],
                streamerFiles = { job['LFN'] : job['LUMI_ID'] },
                runNumber = job['RUN_ID'],
                lumiSections = [ job['LUMI_ID'] ],
                streamID = job['STREAM_ID']
                )
            jobEntityDict[job['JOB_ID']] = jobEntity
        else:
            jobEntity['streamerFiles'][job['LFN']] = job['LUMI_ID']
            jobEntity['lumiSections'].append(job['LUMI_ID'])
            
        # remove duplicates
        jobEntity['lumiSections'] = list(set(jobEntity['lumiSections']))

    return jobEntityDict.values()


def updateJobs(dbConn, jobEntityList):
    """
    _updateJobs_

    update repack job status to Used

    """
    for jobEntity in jobEntityList:
        updateJobDef(dbConn, "Repack", jobEntity["jobID"], "Used")



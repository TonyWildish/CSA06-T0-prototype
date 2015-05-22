#!/usr/bin/env python
"""
_StreamerJob_

Factory object for generating JobSpecs for running on 'streamer' files
This means all files which are stored in the streamer table, which means
all files transfered over from Cessy (do not technically need to be
streamer files, pixdmp files and others are possible as well)

"""

import os
import logging

from T0.JobFactory.FactoryInterface import FactoryInterface


class StreamerJob(FactoryInterface):
    """
    _StreamerJob_

    JobSpec Factory for building repacker job specs


    """
    def __init__(self, workflowSpec):
        FactoryInterface.__init__(self, workflowSpec)

        self.run = self.workflow.parameters['RunNumber']
        self.cmssw = {}
        self.cmssw['Version'] = self.workflow.parameters["CMSSWVersion"]
        self.cmssw['ScramArch'] = self.workflow.parameters['ScramArch']
        self.cmssw['CMSPath'] = self.workflow.parameters['CMSPath']


    def __call__(self, jobType, jobEntity, jobSpecBaseDir):
        """
        _operator()_

        Create a streamer job spec from the workflow template provided
        using the list of streamer files as input to the job.

        """
        jobName = "%s-Run%s-%s" % (jobType, jobEntity["runNumber"], jobEntity['jobID'])

        jobSpec = self.workflow.createJobSpec()

        jobSpecDir = os.path.join(jobSpecBaseDir,
                                  str((jobEntity['jobID'] / 1000) % 1000).zfill(4))
        if not os.path.isdir(jobSpecDir):
            os.makedirs(jobSpecDir)

        jobSpecFileName = jobName + "-jobspec.xml"
        jobSpecFile = os.path.join(jobSpecDir, jobSpecFileName) 

        jobSpec.setJobName(jobName)

        # JobQueue only understand Repack type for now
        # jobSpec.setJobType(jobType)
        jobSpec.setJobType("Repack")

        jobSpec.parameters['RunNumber'] = self.run
        jobSpec.parameters['JobSpecFile'] = jobSpecFile

        cmsswConfig = jobSpec.payload.cfgInterface
        cmsswConfig.addExtension('Streamer', jobEntity)

        #
        # this is redundant information (should we remove this ?)
        #
        
##        # should sort by lumisection id ?
##        sortedList = sorted(jobEntity["streamerFiles"].iteritems(),
##                            key = lambda (k,v):(v,k))

##        # inputStreamers is the list of streamer file name sorted by lumisection number
##        inputStreamers = map(operator.itemgetter(0), sortedList)

##        # extract a sorted list of lumi sections
##        lumiSections = sorted(list(set(jobEntity["streamerFiles"].values())))

##        cmsswConfig.inputStreamers = inputStreamers
##        cmsswConfig.activeStreams = jobEntity["activeOutputModules"]
##        cmsswConfig.inputRun = self.run
##        cmsswConfig.inputLumiSections = lumiSections

        # finally, save the file (PA needs this)
        jobSpec.save(jobSpecFile)
        logging.debug("JobSpec file saved as %s" % jobSpecFile)

        return jobSpec
        
        

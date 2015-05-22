#!/usr/bin/env python
"""
_MergeJob_

Factory object for generating Merge JobSpecs from
a merge workflow spec.
"""

__revision__ = "$Id: MergePackJob.py,v 1.2 2009/03/04 17:22:31 hufnagel Exp $"
__version__ = "$Revision: 1.2 $"

import os
import logging

from T0.JobFactory.FactoryInterface import FactoryInterface

class MergePackJob(FactoryInterface):
    """
    _MergePackJob_

    JobSpec Factory for building mergepack job specs
    
    """
    def __init__(self, workflowSpec):
        FactoryInterface.__init__(self, workflowSpec)
        self.run = self.workflow.parameters["RunNumber"]
        self.cmssw = {}
        self.cmssw["Version"] = self.workflow.parameters["CMSSWVersion"]
        self.cmssw["ScramArch"] = self.workflow.parameters["ScramArch"]
        self.cmssw["CMSPath"] = self.workflow.parameters["CMSPath"]

    def __call__(self, jobID, jobSpecBaseDir, *inputFiles):
        """
        _operator()_

        Create a mergepack job spec from the workflow template provided
        using the list of edm files as input to the job.

        Edm files are sorted by their lumi ID, with files containing
        older lumi sections being inserted first.
        
        """
        jobName = "MergePack-Run%s-%s" % (self.run, jobID)

        jobSpec = self.workflow.createJobSpec()

        jobSpecDir = os.path.join(jobSpecBaseDir,
                                  str((jobID / 1000) % 1000).zfill(4))
        if not os.path.isdir(jobSpecDir):
            os.makedirs(jobSpecDir)

        jobSpecFileName = jobName + "-jobspec.xml"
        jobSpecFile = os.path.join(jobSpecDir, jobSpecFileName) 

        jobSpec.setJobName(jobName)

        # determines how JobQueue treats this job
        # because of crazy runtime handling of merge jobs,
        # need to treat it as a processing job
        jobSpec.setJobType("Repack")

        jobSpec.parameters["RunNumber"] = self.run
        jobSpec.parameters['JobSpecFile'] = jobSpecFile

        cmsswConfig = jobSpec.payload.cfgInterface
        
        # do we want to replace this via the addExtension method ?
        # would mean MergePackJobEntity which is extracted at runtime
        cmsswConfig.inputFiles = inputFiles

        # finally, save the file (PA needs this)
        jobSpec.save(jobSpecFile)
        logging.debug("JobSpec file saved as %s" % jobSpecFile)

        return jobSpec

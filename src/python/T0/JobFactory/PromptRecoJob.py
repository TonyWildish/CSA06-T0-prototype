#!/usr/bin/env python
"""
_PromptRecoJob_

Factory object for generating PromptReco JobSpecs from
a PromptReco workflow spec.
"""

__revision__ = "$Id: PromptRecoJob.py,v 1.15 2009/07/18 00:09:34 hufnagel Exp $"
__version__ = "$Revision: 1.15 $"

import os
import logging

from T0.JobFactory.FactoryInterface import FactoryInterface

class PromptRecoJob(FactoryInterface):
    """
    _PromptRecoJob_

    Factory object for generating PromptReco JobSpecs from
    a PromptReco workflow spec.
    """
    def __init__(self, workflowSpec):
        FactoryInterface.__init__(self, workflowSpec)

        self.run = self.workflow.parameters["RunNumber"]
        self.cmssw = {}
        self.cmssw["Version"] = self.workflow.parameters["CMSSWVersion"]
        self.cmssw["ScramArch"] = self.workflow.parameters["ScramArch"]
        self.cmssw["CMSPath"] = self.workflow.parameters["CMSPath"]

    def __call__(self, jobID, wmbsFiles, maxEvents, skipEvents, jobSpecBaseDir):
        """
        _operator()_

        Create a PromptReco jobspec given a repacked file and a PromptReco
        job id.

        """
        jobName = "PromptReco-Run%s-%s" % (self.run, jobID)

        jobSpec = self.workflow.createJobSpec()

        jobSpecDir = os.path.join(jobSpecBaseDir,
                                  str((jobID / 1000) % 1000).zfill(4))
        if not os.path.isdir(jobSpecDir):
            os.makedirs(jobSpecDir)

        jobSpecFileName = jobName + "-jobspec.xml"
        jobSpecFile = os.path.join(jobSpecDir, jobSpecFileName)

        jobSpec.setJobName(jobName)
        jobSpec.setJobType("Processing")

        jobSpec.parameters["RunNumber"] = self.run
        jobSpec.parameters["MaxEvents"] = maxEvents
        jobSpec.parameters["SkipEvents"] = skipEvents
        jobSpec.parameters['JobSpecFile'] = jobSpecFile

        cmsswConfig = jobSpec.payload.cfgInterface
        cmsswConfig.inputFiles = [ ]

        for wmbsFile in wmbsFiles:
            cmsswConfig.inputFiles.append(wmbsFile["lfn"])

        # finally, save the file (PA needs this)
        jobSpec.save(jobSpecFile)
        logging.debug("JobSpec file saved as %s" % jobSpecFile)

##        outputModules = cfgInterface.outputModules.keys()
##        outMod = cfgInterface.outputModules[outputModules[0]]
##        outMod['catalog'] = "%s-catalog.xml" % jobName
##        outMod['logicalFileName'] = "%s/%s.root" % (outMod['LFNBase'], jobName)

        return jobSpec

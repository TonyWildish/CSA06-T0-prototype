#!/usr/bin/env python
"""
_ProcessingJob_

"""

__revision__ = "$Id: ProcessingJob.py,v 1.1 2009/04/08 20:29:09 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import logging

from T0.JobFactory.FactoryInterface import FactoryInterface

class ProcessingJob(FactoryInterface):
    def __init__(self, workflowSpec):
        FactoryInterface.__init__(self, workflowSpec)
        self.run = self.workflow.parameters["RunNumber"]
        return

    def __call__(self, jobName, fileLFNs, parentLFNs = None, maxEvents = -1, skipEvents = 0):
        """
        _operator()_

        Create a PromptReco jobspec given a repacked file and a PromptReco
        job id.
        """
        jobSpec = self.workflow.createJobSpec()
        jobSpec.parameters["RunNumber"] = self.run
        jobSpec.parameters["MaxEvents"] = maxEvents
        jobSpec.parameters["SkipEvents"] = skipEvents
        jobSpec.setJobType("Processing")
        jobSpec.setJobName(jobName)

        if type(fileLFNs) != list:
            fileLFNs = [fileLFNs]

        cfgInterface = jobSpec.payload.cfgInterface
        cfgInterface.inputFiles = fileLFNs

        if parentLFNs != None:
            if type(parentLFNs) != list:
                parentLFNs = [parentLFNs]

            cfgInterface.addExtension("SecondaryInputFiles", parentLFNs)
            
        return jobSpec

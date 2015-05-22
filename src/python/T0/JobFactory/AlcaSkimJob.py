#!/usr/bin/env python
"""
_AlcaSkimJob_

Factory object for generating AlcaSkim JobSpecs from
a AlcaSkim workflow spec.
"""

__revision__ = "$Id: AlcaSkimJob.py,v 1.5 2009/07/01 12:00:22 gowdy Exp $"
__version__ = "$Revision: 1.5 $"

import logging

from T0.JobFactory.FactoryInterface import FactoryInterface

class AlcaSkimJob(FactoryInterface):
    """
    _AlcaSkimJob_

    Factory object for generating AlcaSkim JobSpecs from
    a AlcaSkim workflow spec.
    """
    def __init__(self, workflowSpec):
        FactoryInterface.__init__(self, workflowSpec)

        self.run = self.workflow.parameters["RunNumber"]
        self.cmssw = {}
        self.cmssw["Version"] = self.workflow.parameters["CMSSWVersion"]
        self.cmssw["ScramArch"] = self.workflow.parameters["ScramArch"]
        self.cmssw["CMSPath"] = self.workflow.parameters["CMSPath"]

    def __call__(self, jobName, maxEvents, skipEvents, fileList):
        """
        _operator()_

        Creator a AlcaSkim jobspec given a alca file and a AlcaSkim
        job id.
        """
        jobSpec = self.workflow.createJobSpec()
        jobSpec.parameters["RunNumber"] = self.run
        jobSpec.parameters["MaxEvents"] = maxEvents
        jobSpec.parameters["SkipEvents"] = skipEvents
        jobSpec.setJobType("Processing")
        jobSpec.setJobName(jobName)

        cfgInterface = jobSpec.payload.cfgInterface
        cfgInterface.inputFiles = []

        for file in fileList:
            cfgInterface.inputFiles.append(file["lfn"])

        outputModules = cfgInterface.outputModules.keys()
        for outputModule in outputModules:
            outMod = cfgInterface.outputModules[outputModule]
            outMod['catalog'] = "%s-catalog.xml" % outputModule
            nameAppend = outputModule.replace("ALCARECOStream","")
            outMod['logicalFileName'] = "%s/%s-%s.root" % (outMod['LFNBase'], jobName, nameAppend)

        return jobSpec

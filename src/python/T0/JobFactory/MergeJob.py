#!/usr/bin/env python
"""
_MergeJob_

Factory object for generating Merge JobSpecs from
a merge workflow spec.
"""

__revision__ = "$Id: MergeJob.py,v 1.21 2009/04/09 16:34:19 sfoulkes Exp $"
__version__ = "$Revision: 1.21 $"

import logging

from ProdCommon.MCPayloads.UUID import makeUUID
from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker

from T0.JobFactory.FactoryInterface import FactoryInterface

class MergeJob(FactoryInterface):
    """
    _MergeJob_

    JobSpec Factory for building merge job specs
    """
    def __init__(self, workflowSpec):
        FactoryInterface.__init__(self, workflowSpec)

        self.jobSpec = None
        self.jobName = None
        self.run = self.workflow.parameters["RunNumber"]
        self.cmssw = {}
        self.cmssw["Version"] = self.workflow.parameters["CMSSWVersion"]
        self.cmssw["ScramArch"] = self.workflow.parameters["ScramArch"]
        self.cmssw["CMSPath"] = self.workflow.parameters["CMSPath"]

    def __call__(self, jobName, fileLFNs, parentLFNs = None, maxEvents = -1, skipEvents = 0):        
        """
        _operator()_

        Create a merge job spec from the workflow template provided
        using the list of repacked files as input to the job.

        Repacked files are sorted by their lumi ID, with files containing
        older lumi sections being inserted first.
        """
        jobSpec = self.workflow.createJobSpec()
        jobSpec.parameters["RunNumber"] = self.run
        jobSpec.setJobName(jobName)
        jobSpec.setJobType("Merge")

        cfgInterface = jobSpec.payload.cfgInterface
        cfgInterface.inputFiles = fileLFNs
        return jobSpec

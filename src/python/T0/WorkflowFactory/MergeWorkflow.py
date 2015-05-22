#!/usr/bin/env python
"""
_MergeWorkflow_

Workflow Factory for merge job workflows
"""

__revision__ = "$Id: MergeWorkflow.py,v 1.28 2009/04/29 19:18:44 sfoulkes Exp $"
__version__ = "$Revision: 1.28 $"

import time
import os

from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools

from T0.WorkflowFactory.FactoryInterface import FactoryInterface
from T0.WorkflowFactory.RepackNaming import getLFN

class MergeWorkflow(FactoryInterface):
    """
    _MergeWorkflow_

    Util to build workflows for merge jobs
    """
    def __init__(self, runNumber, version, cmsPath, scramArch):
        FactoryInterface.__init__(self, version, cmsPath, scramArch)
        self.run = runNumber
        self.workflow = None
        self.timestamp = None
        self.primaryDataset = None
        self.processedDataset = None
        self.dataTier = None
        self.acquisitionEra = None
        self.processingVersion = None
        self.cmsRunNode = None
        self.workflowName = None
        self.useLazyDownload = False
        self.onlySites = None
        self.jobSplitParams = None
        self.workflowClass = None
        self.workflowDescription = None

    def setPrimaryDataset(self, primaryDataset):
        """
        _setPrimaryDataset_

        Set the primary dataset that this workflow will run on.
        """
        self.primaryDataset = primaryDataset
        return

    def setProcessedDataset(self, processedDataset):
        """
        _setProcessedDataset_

        Set the processed dataset that this workflow will run on.
        """
        self.processedDataset = processedDataset
        return

    def setDataTier(self, dataTier):
        """
        _setDataTier_

        Set the data tier that this workflow will work on.
        """
        self.dataTier = dataTier
        return

    def setAcquisitionEra(self, acquisitionEra):
        """
        _setAcquisitionEra_

        Set the current acquisition era.
        """
        self.acquisitionEra = acquisitionEra
        return

    def setProcessingVersion(self, processingVersion):
        """
        _setProcessingVersion_

        Set the processing version of the data that this workflow will merge.
        """
        self.processingVersion = processingVersion
        return

    def setLazyDownload(self, useLazyDownload):
        """
        _setLazyDownload_

        This enables/disables lazy download mode in the framework.
        """
        self.useLazyDownload = useLazyDownload

    def setWorkflowSites(self, siteList):
        """
        _setWorkflowSites_

        Set the sites that the workflow can be run at.
        """
        self.onlySites = siteList
        return

    def setJobSplitParams(self, params):
        """
        _setJobSplitParams_

        Set the job splitting parameters that will be passed to the job
        splitting algorithm.
        """
        self.jobSplitParams = params
        return

    def setWorkflowClass(self, workflowClass):
        """
        _setWorkflowClass_

        Set the workflow class.  This is used to construct job names.
        """
        self.workflowClass = workflowClass
        return

    def setWorkflowDescription(self, description):
        """
        _setWorkflowDescription_

        Set the workflow description.  This is used to construct job names.
        """
        self.workflowDescription = description
        return

    def makeWorkflow(self):
        """
        _makeWorkflow_

        Create a workflow spec instance for the run provided
        """        
        self.timestamp = int(time.time())
        self.workflow = WorkflowSpec()
        
        self.workflowName = "Merge-Run%s-%s-%s-%s" % (self.run, self.dataTier,
                                                      self.primaryDataset,
                                                      self.processedDataset)
        self.workflow.setWorkflowName(self.workflowName)
        self.workflow.setRequestCategory("data")
        self.workflow.setRequestTimestamp(self.timestamp)
        self.workflow.parameters["WorkflowType"] = "Merge"
        self.workflow.parameters["ProdRequestID"] = self.run
        self.workflow.parameters["RunNumber"] = self.run
        self.workflow.parameters["WorkflowRunNumber"] = self.run        
        self.workflow.parameters["DataTier"] = self.dataTier
        self.workflow.parameters["CMSSWVersion"] = self.cmssw["CMSSWVersion"] 
        self.workflow.parameters["ScramArch"] = self.cmssw["ScramArch"] 
        self.workflow.parameters["CMSPath"] = self.cmssw["CMSPath"]
        self.workflow.parameters["OnlySites"] = self.onlySites

        self.workflow.parameters["WorkflowClass"] = self.workflowClass
        self.workflow.parameters["WorkflowDescription"] = self.workflowDescription

        if self.jobSplitParams != None:
            self.workflow.parameters.update(self.jobSplitParams)

        if self.useLazyDownload == True:
            self.workflow.parameters["UseLazyDownload"] = "True"
        else:
            self.workflow.parameters["UseLazyDownload"] = "False"            
        
        self.cmsRunNode = self.workflow.payload
        self.cmsRunNode.name = "cmsRun1"
        self.cmsRunNode.type = "CMSSW"
        self.cmsRunNode.application["Version"] = self.cmssw["CMSSWVersion"]
        self.cmsRunNode.application["Executable"] = "cmsRun"
        self.cmsRunNode.application["Project"] = "CMSSW"
        self.cmsRunNode.application["Architecture"] = self.cmssw["ScramArch"]

        preExecScript = self.cmsRunNode.scriptControls["PreExe"]
        preExecScript.append("T0.Tier0Merger.RuntimeTier0Merger")
        
        inputDataset = self.cmsRunNode.addInputDataset(self.primaryDataset,
                                                       self.processedDataset)
        inputDataset["DataTier"] = self.dataTier

        outputDataset = self.cmsRunNode.addOutputDataset(self.primaryDataset,
                                                         self.processedDataset,
                                                         "Merged")
        outputDataset["DataTier"] = self.dataTier
        outputDataset["ApplicationName"] = "cmsRun"
        outputDataset["ApplicationProject"] = "CMSSW"
        outputDataset["ApplicationVersion"] = self.cmssw["CMSSWVersion"]
        outputDataset["ApplicationFamily"] = "Merged"
        outputDataset["ParentDataset"] = "/%s/%s/%s" % (self.primaryDataset,
                                                        self.processedDataset,
                                                        self.dataTier)

        self.workflow.payload.cfgInterface = CMSSWConfig()
        cfgInt = self.workflow.payload.cfgInterface
        cfgInt.sourceType = "PoolSource"
        cfgInt.maxEvents["input"] = -1
        cfgInt.configMetadata["name"] = self.workflowName
        cfgInt.configMetadata["version"] = "AutoGenerated"
        cfgInt.configMetadata["annotation"] = "AutoGenerated By Tier 0"
        
        outputModule = cfgInt.getOutputModule("Merged")
        outputModule["catalog"] = "%s-Catalog.xml" % outputModule["Name"]
        outputModule["primaryDataset"] = self.primaryDataset
        outputModule["processedDataset"] = self.processedDataset
        outputModule["dataTier"] = self.dataTier
        outputModule["acquisitionEra"] = self.acquisitionEra
        outputModule["processingVersion"] = self.processingVersion

        outputDataset["LFNBase"] = getLFN(outputModule, self.run)
        outputModule["LFNBase"] = outputDataset["LFNBase"]
        outputModule["fileName"] = "%s.root" % outputModule["Name"]

        outputModule["logicalFileName"] = os.path.join(
            outputDataset["LFNBase"], "Merged.root")

        WorkflowTools.addStageOutNode(self.cmsRunNode, "stageOut1")
        WorkflowTools.addLogArchNode(self.cmsRunNode, "logArchive")
        WorkflowTools.generateFilenames(self.workflow)
        
        return self.workflow

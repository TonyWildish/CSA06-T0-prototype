#!/usr/bin/env python
"""
_SkimminWorkflow_

Factory for skimming job workflows
"""

__revision__ = "$Id: SkimmingWorkflow.py,v 1.4 2009/04/29 19:18:44 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

import time
import os
import imp
import logging

from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWAPILoader import CMSSWAPILoader

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools

from T0.WorkflowFactory.FactoryInterface import FactoryInterface

class SkimmingWorkflow(FactoryInterface):
    """
    _SkimmingWorkflow_

    Factory to build workflows for skimming jobs.
    """
    def __init__(self, runNumber, version, cmsPath, scramArch):
        FactoryInterface.__init__(self, version, cmsPath, scramArch)
        self.run = runNumber
        self.workflow = None
        self.timestamp = None
        self.cmsRunNode = None
        self.workflowName = None
        self.useLazyDownload = False
        self.parentPrimaryDataset = None
        self.parentProcessedDataset = None
        self.parentDataTier = None
        self.acquisitionEra = None
        self.processingVersion = None
        self.configFile = None
        self.skimName = None
        self.onlySites = None
        self.jobSplitParams = None
        self.workflowClass = None
        self.workflowDescription = None

    def setSkimName(self, skimName):
        """
        _setSkimName_

        Set the name of the skim, used in naming the workflow.
        """
        self.skimName = skimName
        return

    def setParentDataset(self, primaryDatasetName, processedDatasetName,
                         dataTierName):
        """
        _setParentDataset_

        Set the datasets of the input files that this workflow will run over.
        """
        self.parentPrimaryDataset = primaryDatasetName
        self.parentProcessedDataset = processedDatasetName
        self.parentDataTier = dataTierName
        return

    def setAcquisitionEra(self, acquisitionEra):
        """
        _setAcquisitionEra_

        Set the acquisition era.
        """
        self.acquisitionEra = acquisitionEra
        return

    def setProcessingVersion(self, processingVersion):
        """
        _setProcessingVersion_

        Set the processing version.
        """
        self.processingVersion = processingVersion
        return

    def setLazyDownload(self, useLazyDownload):
        """
        _setLazyDownload_

        This enables/disables lazy download mode in the framework.
        """
        self.useLazyDownload = useLazyDownload

    def setConfigFile(self, configFile):
        """
        _setConfigFile_

        Set the config file that will be used.
        """
        self.configFile = configFile

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
    
    def loadConfig(self):
        """
        _loadConfig_

        Load a framework configuration from a file and return the process
        object.
        """
        loader = CMSSWAPILoader(self.cmssw['ScramArch'],
                                self.cmssw['CMSSWVersion'],
                                self.cmssw['CMSPath'])

        try:
            loader.load()
        except Exception, ex:
            logging.error("Couldn't load CMSSW libraries: %s" % ex)
            return None

        import FWCore.ParameterSet.Config as cms

        self.process = self.createProcessFromFile(self.configFile,
                                                  stripOutputModules = False)

        self.process.source = cms.Source("PoolSource",
                                         fileNames = cms.untracked.vstring())

        cfgInterface = CMSSWConfig()
        cfgInterface.loadConfiguration(self.process)
        loader.unload()
        return cfgInterface

    def initializeOutputModules(self):
        """
        _initializeOutputModules_

        Create the outputModule and outputDataset sections of the workflow.
        """
        cfgWrapper = self.workflow.payload.cfgInterface
        outputModuleNames = cfgWrapper.outputModules.keys()

        for outputModuleName in outputModuleNames:
            outputModule = cfgWrapper.getOutputModule(outputModuleName)
            
            if not outputModule.has_key("filterName"):
                logging.error("No filter name in config")
                return
            if not outputModule.has_key("dataTier"):
                logging.error("No output data tier in config")
                return            
                
            childProcessedDataset = "%s-%s-%s" % (self.acquisitionEra,
                                                  outputModule["filterName"],
                                                  self.processingVersion)
                
            outputModule["catalog"] = '%s-Catalog.xml' % outputModule["Name"]
            outputModule["primaryDataset"] = self.parentPrimaryDataset
            outputModule["processedDataset"] = childProcessedDataset
            outputModule["dataTier"] = outputModule["dataTier"]
            outputModule["acquisitionEra"] = self.acquisitionEra
            outputModule["processingVersion"] = self.processingVersion
            outputModule["LFNBase"] = self.getLFN(outputModule, Unmerged = True)
            outputModule["MergedLFNBase"] = self.getLFN(outputModule)
            outputModule["fileName"] = "%s.root" % outputModule["Name"]
            outputModule["logicalFileName"] = os.path.join(
                outputModule["LFNBase"], "Processing.root")

            outputDataset = self.cmsRunNode.addOutputDataset(self.parentPrimaryDataset,
                                                             childProcessedDataset,
                                                             outputModuleName)
            outputDataset["DataTier"] = outputModule["dataTier"]
            outputDataset["ApplicationName"] = "cmsRun"
            outputDataset["ApplicationProject"] = "CMSSW"
            outputDataset["ApplicationVersion"] = self.cmssw["CMSSWVersion"]
            outputDataset["ApplicationFamily"] = outputModuleName
            outputDataset["LFNBase"] = outputModule["LFNBase"]
            outputDataset["MergedLFNBase"] = outputModule["MergedLFNBase"]
            outputDataset["ParentDataset"] = "/%s/%s/%s" % (self.parentPrimaryDataset,
                                                            self.parentProcessedDataset,
                                                            self.parentDataTier)
            
        return
    
    def makeWorkflow(self):
        """
        _makeWorkflow_

        Generate a workflow.  If the self.configFile parameter has been set
        this will attempt to load the config from file, otherwise it will
        create an empty process object which will get filled in by the runtime
        script.
        """
        self.timestamp = int(time.time())
        self.workflow = WorkflowSpec()
        self.workflowName = "Skim-Run%s-%s-%s-%s-%s" % (self.run, self.skimName,
                                                        self.parentPrimaryDataset,
                                                        self.parentProcessedDataset,
                                                        self.parentDataTier)

        self.workflow.setWorkflowName(self.workflowName)
        self.workflow.setRequestCategory("data")
        self.workflow.setRequestTimestamp(self.timestamp)
        self.workflow.parameters["WorkflowType"] = "Processing"
        self.workflow.parameters["ProdRequestID"] = self.run
        self.workflow.parameters["RunNumber"] = self.run
        self.workflow.parameters["WorkflowRunNumber"] = self.run        
        self.workflow.parameters["CMSSWVersion"] = self.cmssw["CMSSWVersion"] 
        self.workflow.parameters["ScramArch"] = self.cmssw["ScramArch"] 
        self.workflow.parameters["CMSPath"] = self.cmssw["CMSPath"]
        self.workflow.parameters["OnlySites"] = self.onlySites
        self.workflow.parameters["WorkflowClass"] = self.workflowClass
        self.workflow.parameters["WorkflowDescription"] = self.workflowDescription

        self.workflow.parameters.update(self.jobSplitParams)

        self.cmsRunNode = self.workflow.payload
        self.cmsRunNode.name = "cmsRun1"
        self.cmsRunNode.type = "CMSSW"
        self.cmsRunNode.application["Version"] = self.cmssw["CMSSWVersion"]
        self.cmsRunNode.application["Executable"] = "cmsRun"
        self.cmsRunNode.application["Project"] = "CMSSW"
        self.cmsRunNode.application["Architecture"] = self.cmssw["ScramArch"]

        inputDataset = self.cmsRunNode.addInputDataset(self.parentPrimaryDataset,
                                                       self.parentProcessedDataset)
        inputDataset["DataTier"] = self.parentDataTier

        self.workflow.payload.cfgInterface = self.loadConfig()
        self.initializeOutputModules()

        WorkflowTools.addStageOutNode(self.cmsRunNode, "stageOut1")
        WorkflowTools.addLogArchNode(self.cmsRunNode, "logArchive")

        return self.workflow

#!/usr/bin/env python
"""
_PromptRecoWorkflow_

Factory for Promptreco job workflows
"""

__revision__ = "$Id: PromptRecoWorkflow.py,v 1.29 2009/07/01 13:40:56 sfoulkes Exp $"
__version__ = "$Revision: 1.29 $"

import time
import os
import imp
import logging

from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWAPILoader import CMSSWAPILoader

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools

from T0.WorkflowFactory.FactoryInterface import FactoryInterface
from T0.WorkflowFactory.RepackNaming import getLFN

class PromptRecoWorkflow(FactoryInterface):
    """
    _PromptWorkflow_

    Factory to build workflows for PromptReco jobs.
    """
    def __init__(self, runNumber, version, cmsPath, scramArch):
        FactoryInterface.__init__(self, version, cmsPath, scramArch)
        self.run = runNumber
        self.workflow = None
        self.timestamp = None
        self.cmsRunNode = None
        self.workflowName = None
        self.configFile = None
        self.useLazyDownload = False
        self.primaryDataset = None
        self.processedDataset = None
        self.parentProcessedDataset = None
        self.acquisitionEra = None
        self.processingVersion = None

    def setConfigFile(self, configFile):
        """
        _setConfigFile_

        Set the config file that will be loaded into the workflow.
        """
        self.configFile = configFile

    def setPrimaryDataset(self, primaryDatasetName):
        """
        _setPrimaryDataset_

        Set the primary dataset that this workflow will run over.
        """
        self.primaryDataset = primaryDatasetName
        return

    def setProcessedDataset(self, processedDatasetName):
        """
        _setProcessedDataset_

        Set the processed dataset that this workflow will produce.
        """
        self.processedDataset = processedDatasetName
        return

    def setParentProcessedDataset(self, parentProcessedDatasetName):
        """
        _setParentProcessedDataset_

        Set the parent processed dataset for this workflow.
        """
        self.parentProcessedDataset = parentProcessedDatasetName
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
    
    def setupOutputModule(self, outputModuleName, dataTier):
        """
        _setupOutputModule_

        Create the outputModule and outputDataset sections of the workflow.
        """
        outputDataset = self.cmsRunNode.addOutputDataset(self.primaryDataset,
                                                         self.processedDataset,
                                                         outputModuleName)
        outputDataset["NoMerge"] = "True"
        outputDataset["DataTier"] = dataTier
        outputDataset["ApplicationName"] = "cmsRun"
        outputDataset["ApplicationProject"] = "CMSSW"
        outputDataset["ApplicationVersion"] = self.cmssw["CMSSWVersion"]
        outputDataset["ApplicationFamily"] = outputModuleName
        outputDataset["ParentDataset"] = "/%s/%s/%s" % (self.primaryDataset,
                                                        self.parentProcessedDataset,
                                                        "RAW")

        cfgWrapper = self.workflow.payload.cfgInterface
        outputModule = cfgWrapper.getOutputModule(outputModuleName)

        outputModule["catalog"] = '%s-Catalog.xml' % outputModule['Name']
        outputModule["primaryDataset"] = self.primaryDataset
        outputModule["processedDataset"] = self.processedDataset
        outputModule["dataTier"] = dataTier
        outputModule["acquisitionEra"] = self.acquisitionEra
        outputModule["processingVersion"] = self.processingVersion

        outputDataset["LFNBase"] = getLFN(outputModule, self.run, Unmerged = True)
        outputDataset["MergedLFNBase"] = getLFN(outputModule, self.run)
        outputModule["LFNBase"] = outputDataset["LFNBase"]
        outputModule["MergedLFNBase"] = outputDataset["MergedLFNBase"]

        outputModule["fileName"] = "%s.root" % outputModule['Name']

        outputModule["logicalFileName"] = os.path.join(
            outputDataset["LFNBase"], "PromptReco.%s.root" % dataTier)

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
        self.workflowName = "PromptReco-Run%s-%s" % (self.run,
                                                     self.primaryDataset)

        self.workflow.setWorkflowName(self.workflowName)
        self.workflow.setRequestCategory("data")
        self.workflow.setRequestTimestamp(self.timestamp)
        self.workflow.parameters["WorkflowType"] = "Processing"
        self.workflow.parameters["ProdRequestID"] = self.run
        self.workflow.parameters["RunNumber"] = self.run
        self.workflow.parameters["CMSSWVersion"] = self.cmssw["CMSSWVersion"] 
        self.workflow.parameters["ScramArch"] = self.cmssw["ScramArch"] 
        self.workflow.parameters["CMSPath"] = self.cmssw["CMSPath"]

        self.cmsRunNode = self.workflow.payload
        self.cmsRunNode.name = "cmsRun1"
        self.cmsRunNode.type = "CMSSW"
        self.cmsRunNode.application["Version"] = self.cmssw["CMSSWVersion"]
        self.cmsRunNode.application["Executable"] = "cmsRun"
        self.cmsRunNode.application["Project"] = "CMSSW"
        self.cmsRunNode.application["Architecture"] = self.cmssw["ScramArch"]

        inputDataset = self.cmsRunNode.addInputDataset(self.primaryDataset,
                                                       self.parentProcessedDataset)
        inputDataset["DataTier"] = "RAW"
        
        if self.configFile == None:
            self.loadProcessFromFramework()            
        else:
            self.loadProcessFromFile()

        WorkflowTools.addStageOutNode(self.cmsRunNode, "stageOut1")
        WorkflowTools.addLogArchNode(self.cmsRunNode, "logArchive")
        WorkflowTools.generateFilenames(self.workflow)

        return self.workflow

    def loadProcessFromFile(self):
        """
        _loadProcessFromFile_

        Load the config file into the workflow.
        """
        preExecScript = self.cmsRunNode.scriptControls["PreExe"]
        preExecScript.append("T0.PromptRecoInjector.RuntimePromptReco")
        
        cfgBaseName = os.path.basename(self.configFile).replace(".py", "")
        cfgDirName = os.path.dirname(self.configFile)
        modPath = imp.find_module(cfgBaseName, [cfgDirName])

        loader = CMSSWAPILoader(self.cmssw["ScramArch"],
                                self.cmssw["CMSSWVersion"],
                                self.cmssw["CMSPath"])
        
        try:
            loader.load()
        except Exception, ex:
            logging.error("Couldn't load CMSSW libraries: %s" % ex)
            return None
        
        try:
            modRef = imp.load_module(cfgBaseName, modPath[0],
                                     modPath[1], modPath[2])
        except Exception, ex:
            logging.error("Can't load config: %s" % ex)
            loader.unload()
            return None

        import FWCore.ParameterSet.Config as cms            
        cmsCfg = modRef.process

        if self.useLazyDownload == True:
            logging.debug("Lazy downloads ENABLED.")
            cmsCfg.AdaptorConfig = cms.Service("AdaptorConfig",
                                               cacheHint = cms.untracked.string("lazy-download"),
                                               readHint = cms.untracked.string("auto-detect"))
        else:
            logging.debug("Lazy downloads DISABLED.")
            cmsCfg.AdaptorConfig = cms.Service("AdaptorConfig",
                                               cacheHint = cms.untracked.string("application-only"),
                                               readHint = cms.untracked.string("direct-unbuffered"))            

        for outputModuleName in cmsCfg.outputModules:
            outputModule = getattr(cmsCfg, outputModuleName)
            outputModule.fastCloning = cms.untracked.bool(False)
        
        cfgWrapper = CMSSWConfig()
        cfgWrapper.originalCfg = file(self.configFile).read()
        cfgInt = cfgWrapper.loadConfiguration(cmsCfg)
        cfgInt.validateForProduction()
        self.workflow.payload.cfgInterface = cfgWrapper

        for outputModuleName in cmsCfg.outputModules:
            outputModule = getattr(cmsCfg, outputModuleName)            
            self.setupOutputModule(outputModuleName,
                                   outputModule.dataset.dataTier.value())

        loader.unload()
        
        return

    def loadProcessFromFramework(self):
        """
        _loadProcessFromFramework_

        Create an empty process object and load that into the workflow.
        """
        preExecScript = self.cmsRunNode.scriptControls["PreExe"]
        preExecScript.append("T0.PromptRecoInjector.RuntimePromptReco")
        
        cfgWrapper = CMSSWConfig()

        loader = CMSSWAPILoader(self.cmssw["ScramArch"],
                                self.cmssw["CMSSWVersion"],
                                self.cmssw["CMSPath"])
         
        try:
            loader.load()
        except Exception, ex:
            logging.error("Couldn't load CMSSW libraries: %s" % ex)
            return None

        import FWCore.ParameterSet.Config as cms
        import FWCore.ParameterSet.Types as CmsTypes        

        process = cms.Process("PromptReco")
        process.source = cms.Source("PoolSource",
                                    fileNames = cms.untracked.vstring("NOTSET"))

        if self.useLazyDownload == True:
            logging.debug("Lazy downloads ENABLED.")
            import FWCore.ParameterSet.Config as cms            
            process.AdaptorConfig = cms.Service("AdaptorConfig",
                                                cacheHint = cms.untracked.string("lazy-download"),
                                                readHint = cms.untracked.string("auto-detect"))
        else:
            logging.debug("Lazy downloads DISABLED.")
            import FWCore.ParameterSet.Config as cms            
            process.AdaptorConfig = cms.Service("AdaptorConfig",
                                                cacheHint = cms.untracked.string("application-only"),
                                                readHint = cms.untracked.string("direct-unbuffered"))
            
        process.RECO = cms.OutputModule("PoolOutputModule")
        process.RECO.dataset = cms.untracked(cms.PSet())
        process.RECO.dataset.dataTier = cms.untracked(cms.string("RECO"))
        process.RECO.fileName = cms.untracked.string("NOTSET")
        process.RECO.logicalFileName = cms.untracked.string("NOTSET")
        process.RECO.fastCloning = cms.untracked.bool(False)
        process.outpath = cms.EndPath(process.RECO)

        configName = "prompt-reco-config"
        configVersion = "%s-%s-%s" % (self.cmssw["CMSSWVersion"], self.run,
                                      self.primaryDataset)
        configAnnot = "auto generated prompt reco config"

        process.configurationMetadata = CmsTypes.untracked(CmsTypes.PSet())
        process.configurationMetadata.name = CmsTypes.untracked(CmsTypes.string(configName))
        process.configurationMetadata.version = CmsTypes.untracked(CmsTypes.string(configVersion))
        process.configurationMetadata.annotation = CmsTypes.untracked(CmsTypes.string(configAnnot))

        cfgInt = cfgWrapper.loadConfiguration(process)
        cfgWrapper.conditionsTag = "NOTSET"
        cfgInt.validateForProduction()

        setattr(self.workflow.payload, "cfgInterface", cfgWrapper)

        loader.unload()
        
        return

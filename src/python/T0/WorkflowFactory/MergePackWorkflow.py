#!/usr/bin/env python
"""
_MergePackWorkflow_

Workflow Factory for mergepacking workflows
"""

__revision__ = "$Id: MergePackWorkflow.py,v 1.5 2009/06/09 14:50:54 hufnagel Exp $"
__version__ = "$Revision: 1.5 $"

import os
import time
import logging

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools

from T0.WorkflowFactory.FactoryInterface import FactoryInterface


##class NodeFinder:
##    """
##    Util to search a workflow for a node by name

##    """
##    def __init__(self, nodeName):
##        self.nodeName = nodeName
##        self.result = None

##    def __call__(self, nodeInstance):
##        if nodeInstance.name == self.nodeName:
##            self.result = nodeInstance

class MergePackWorkflow(FactoryInterface):
    """
    _MergePackWorkflow_

    Util to build workflows for mergepack jobs
    """
    def __init__(self, runNumber, version, cmsPath, scramArch, *outModuleInfo):
        FactoryInterface.__init__(self, version, cmsPath, scramArch)
        self.run = runNumber
        self.outputModules = list(outModuleInfo)


    def buildConfiguration(self, enableLazyDownload, configFile):
        """
        _buildConfiguration_

        mostly just a method to take the passed in information

        """
        outputModuleDetails = {}

        for moduleInfo in self.outputModules:

            moduleName = "write_%s_%s_%s" % (moduleInfo["stream"],
                                             moduleInfo["dataset"],
                                             moduleInfo["dataTier"])

            outputModuleDetails[moduleName] = {
                "Stream" : moduleInfo["stream"],
                "primaryDataset" : moduleInfo.get("dataset", None),
                "processedDataset" : moduleInfo.get("processedDataset", None),
                "dataTier" : moduleInfo["dataTier"],
                "acquisitionEra" : moduleInfo["acquisitionEra"],
                "processingVersion" : moduleInfo["processingVersion"],
                }

            if moduleInfo.has_key("triggerPaths"):
                selEvents = [ "%s:%s" % (x, moduleInfo["process"])
                              for x in moduleInfo["triggerPaths"] ]
                outputModuleDetails[moduleName]["SelectEvents"] = selEvents
                outputModuleDetails[moduleName]["compressionLevel"] = 3

        if configFile == None:

            cfgInterface = self.createConfiguration(sourceType = "PoolSource",
                                                    processName = "MERGEPACKER",
                                                    configName = "mergepacker-config",
                                                    enableLazyDownload = enableLazyDownload,
                                                    outputModuleDetails = outputModuleDetails,
                                                    noEventSort = True)

        else:

            cfgInterface = self.createConfiguration(sourceType = "PoolSource",
                                                    configFile = configFile,
                                                    enableLazyDownload = enableLazyDownload,
                                                    outputModuleTemplate = outputModuleDetails.values()[0],
                                                    noEventSort = True)
                
        return cfgInterface

    
    def makeWorkflowSpec(self, name, enableLazyDownload, configFile = None):
        """
        _makeWorkflowSpec_

        Create a workflow spec instance
        
        """
        self.workflow = WorkflowSpec()
        self.workflow.setWorkflowName(name)
        self.workflow.setRequestCategory("data")
        self.workflow.setRequestTimestamp(int(time.time()))
        self.workflow.parameters["WorkflowType"] = "Repack"
        self.workflow.parameters["RequestLabel"] = name
        self.workflow.parameters["ProdRequestID"] = self.run
        self.workflow.parameters["RunNumber"] = self.run
        self.workflow.parameters["CMSSWVersion"] = self.cmssw["CMSSWVersion"] 
        self.workflow.parameters["ScramArch"] = self.cmssw["ScramArch"] 
        self.workflow.parameters["CMSPath"] = self.cmssw["CMSPath"]

        cmsRunNode = self.workflow.payload
        cmsRunNode.name = "cmsRun1"
        cmsRunNode.type = "CMSSW"
        cmsRunNode.application["Version"] = self.cmssw["CMSSWVersion"]
        cmsRunNode.application["Executable"] = "cmsRun"
        cmsRunNode.application["Project"] = "CMSSW"
        cmsRunNode.application["Architecture"] = self.cmssw["ScramArch"]

        # runtime express merge script
        cmsRunNode.scriptControls["PreExe"].append(
            "T0.ExpressMerger.RuntimeExpressMerger"
            )

        # build the configuration template for the workflow
        cmsRunNode.cfgInterface = self.buildConfiguration(enableLazyDownload, configFile)
        if cmsRunNode.cfgInterface == None:
            return None

        # generate Dataset information for workflow from cfgInterface
        for outMod,moduleInstance in cmsRunNode.cfgInterface.outputModules.items():
            primaryName = moduleInstance["primaryDataset"] 
            processedName = moduleInstance["processedDataset"] 

            outDS = cmsRunNode.addOutputDataset(primaryName,
                                                processedName,
                                                outMod)
            
            outDS["DataTier"] = moduleInstance["dataTier"]
            outDS["ApplicationName"] = cmsRunNode.application["Executable"]
            outDS["ApplicationFamily"] = outMod
            outDS["PhysicsGroup"] = "Tier0"
            outDS["ApplicationFamily"] = outMod

            # generate just single LFN stub (all output is merged)
            # insert them into the output module and dataset info
            outDS["LFNBase"] = self.getLFN(moduleInstance, dataType = "express")
            moduleInstance["LFNBase"] = outDS["LFNBase"]
            moduleInstance["logicalFileName"] = os.path.join(
                outDS["LFNBase"], "%s.root" % outMod
                )

        WorkflowTools.addStageOutNode(cmsRunNode, "stageOut1")
        WorkflowTools.addLogArchNode(cmsRunNode, "logArchive")

        # override stageout
        #
        # FIXME: This hardcodes the TFC LFN prefix !!!
##        if svcClass != None:
##            finder = NodeFinder("stageOut1")
##            self.workflow.payload.operate(finder)
##            node = finder.result

##            WorkflowTools.addStageOutOverride(node,
##                                              "rfcp",
##                                              "",
##                                              "srm-cms.cern.ch",
##                                              "rfio:///castor?svcClass=%s&path=/castor/cern.ch/cms" % svcClass)

        return self.workflow

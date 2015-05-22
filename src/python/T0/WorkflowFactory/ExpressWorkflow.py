#!/usr/bin/env python
"""
_ExpressWorkflow_

Workflow Factory for Express processing workflows

This workflow should contain a template config and entries
for all split and accumulator datasets being processed in a
given run.

A blank configuration template is generated that gets pruned down
when job specs are created.
"""

__revision__ = "$Id: "
__version__ = "$Revision: "

import os
import time
import logging

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools

from T0.WorkflowFactory.FactoryInterface import FactoryInterface


class ExpressWorkflow(FactoryInterface):
    """
    _ExpressFactory_

    Util to build workflows for express processing jobs
    
    """
    def __init__(self, runNumber, version, globalTag, cmsPath, scramArch, *outModuleInfo):
        FactoryInterface.__init__(self, version, cmsPath, scramArch)
        self.run = runNumber
        self.outputModules = list(outModuleInfo)
        self.globalTag = globalTag


    def buildConfiguration(self, configFile, enableLazyDownload):
        """
        _buildConfiguration_

        mostly just a method to take the passed in information

        """
        outputModuleDetails = {}

        for moduleInfo in self.outputModules:

            if moduleInfo.has_key("dataset"):
                moduleName = "write_%s_%s_%s" % (moduleInfo["stream"],
                                                 moduleInfo["dataset"],
                                                 moduleInfo["dataTier"])
            else:
                moduleName = "write_%s_%s" % (moduleInfo["stream"],
                                              moduleInfo["dataTier"])

            outputModuleDetails[moduleName] = {
                "Stream" : moduleInfo["stream"],
                "primaryDataset" : moduleInfo.get("dataset", None),
                "processedDataset" : moduleInfo.get("processedDataset", None),
                "dataTier" : moduleInfo["dataTier"],
                "acquisitionEra" : moduleInfo["acquisitionEra"],
                "processingVersion" : moduleInfo["processingVersion"],
##                "globalTag" : moduleInfo["globalTag"],
                "compressionLevel" : 3
                }

            if moduleInfo.has_key("triggerPaths"):
                selEvents = [ "%s:%s" % (x, moduleInfo["process"])
                              for x in moduleInfo["triggerPaths"] ]
                outputModuleDetails[moduleName]["SelectEvents"] = selEvents
            else:
                outputModuleDetails[moduleName]["SelectEvents"] = None

        cfgInterface = self.createConfiguration(sourceType = "NewEventStreamFileReader",
                                                configFile = configFile,
                                                enableLazyDownload = enableLazyDownload,
                                                outputModuleDetails = outputModuleDetails,
                                                setEventContentInOutput = True,
                                                compressionLevel = 3)

        return cfgInterface


    def makeWorkflowSpec(self, name, configFile, enableLazyDownload):
        """
        _makeWorkflowSpec_

        Create a workflow spec instance

        """
        #  //
        # // Initialise basic workflow
        #//
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

        # runtime support for StreamerJobEntity
        self.workflow.addPythonLibrary("T0.DataStructs")

        cmsRunNode = self.workflow.payload
        cmsRunNode.name = "cmsRun1"
        cmsRunNode.type = "CMSSW"
        cmsRunNode.application["Version"] = self.cmssw["CMSSWVersion"]
        cmsRunNode.application["Executable"] = "cmsRun"
        cmsRunNode.application["Project"] = "CMSSW"
        cmsRunNode.application["Architecture"] = self.cmssw["ScramArch"]

        # runtime express script
        cmsRunNode.scriptControls["PreExe"].append(
            "T0.ExpressInjector.RuntimeExpress")
        
        # build the configuration template for the workflow
        cmsRunNode.cfgInterface = self.buildConfiguration(configFile, enableLazyDownload)
        if cmsRunNode.cfgInterface == None:
            return None

        # override global tag
        cmsRunNode.cfgInterface.conditionsTag = self.globalTag

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

            # generate just single LFN stub (all output is unmerged)
            # insert them into the output module and dataset info
            outDS["LFNBase"] = self.getLFN(moduleInstance, dataType = 'express', Unmerged = True)
            moduleInstance["LFNBase"] = outDS["LFNBase"]
            moduleInstance["logicalFileName"] = os.path.join(
                outDS["LFNBase"], "%s.root" % outMod
                )

        WorkflowTools.addStageOutNode(cmsRunNode, "stageOut1")
        WorkflowTools.addLogArchNode(cmsRunNode, "logArchive")
        
        return self.workflow

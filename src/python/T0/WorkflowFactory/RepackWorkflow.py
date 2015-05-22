#!/usr/bin/env python
"""
_RepackWorkflow_

Workflow Factory for Repacker job workflows

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


class RepackWorkflow(FactoryInterface):
    """
    _RepackFactory_

    Util to build workflows for accumulator or merge repacker jobs
    
    """
    def __init__(self, runNumber, version, cmsPath, scramArch, *outModuleInfo):
        FactoryInterface.__init__(self, version, cmsPath, scramArch)
        self.run = runNumber
        self.outputModules = list(outModuleInfo)


    def buildConfiguration(self, enableLazyDownload):
        """
        _buildConfiguration_

        Using a RepackerConfigMaker instance, generate a template
        config file

        """
        outputModuleDetails = {}

        for moduleInfo in self.outputModules:

            moduleName = "write_%s_%s_%s" % (moduleInfo["stream"],
                                             moduleInfo["dataset"],
                                             moduleInfo["dataTier"])

            outputModuleDetails[moduleName] = {
                "Stream" : moduleInfo["stream"],
                "algorithm" : None,
                "primaryDataset" : moduleInfo.get("dataset", None),
                "processedDataset" : moduleInfo.get("processedDataset", None),
                "dataTier" : moduleInfo["dataTier"],
                "filterName" : None,
                "acquisitionEra" : moduleInfo["acquisitionEra"],
                "processingVersion" : moduleInfo["processingVersion"],
                "globalTag" : moduleInfo["globalTag"],
                "LFNBase" : None,
                "MergedLFNBase" : None,
                "compressionLevel" : 6
                }

            if moduleInfo.has_key("triggerPaths"):
                selEvents = [ "%s:%s" % (x, moduleInfo["process"])
                              for x in moduleInfo["triggerPaths"] ]
                outputModuleDetails[moduleName]["SelectEvents"] = selEvents
            else:
                outputModuleDetails[moduleName]["SelectEvents"] = None

        cfgInterface = self.createConfiguration(sourceType = "NewEventStreamFileReader",
                                                processName = "REPACKER",
                                                configName = "repack-config",
                                                enableLazyDownload = enableLazyDownload,
                                                outputModuleDetails = outputModuleDetails)

        return cfgInterface


    def makeWorkflow(self, name, enableLazyDownload):
        """
        _makeWorkflow_

        Create a workflow spec instance for the run provided

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

        # runtime repacker script
        cmsRunNode.scriptControls["PreExe"].append(
            "T0.RepackerInjector.RuntimeRepacker")
        
        # build the configuration template for the workflow
        cmsRunNode.cfgInterface = self.buildConfiguration(enableLazyDownload)
        if cmsRunNode.cfgInterface == None:
            return None

        # generate Dataset information for workflow from cfgInterface
        for outMod in cmsRunNode.cfgInterface.outputModules.keys():
            moduleInstance = cmsRunNode.cfgInterface.getOutputModule(outMod)
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

            # generate merged and unmerged LFN stubs
            # insert them into the output module and dataset info
            outDS["LFNBase"] = self.getLFN(moduleInstance, Unmerged = True)
            outDS["MergedLFNBase"] = self.getLFN(moduleInstance)
            moduleInstance["LFNBase"] = outDS["LFNBase"]
            moduleInstance["MergedLFNBase"] = outDS["MergedLFNBase"]
            moduleInstance["logicalFileName"] = os.path.join(
                outDS["LFNBase"], "%s.root" % outMod)

        WorkflowTools.addStageOutNode(cmsRunNode, "stageOut1")
        WorkflowTools.addLogArchNode(cmsRunNode, "logArchive")
        
        return self.workflow

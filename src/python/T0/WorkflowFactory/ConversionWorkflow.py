#!/usr/bin/env python
"""
_ConversionWorkflow_

Workflow Factory for Conversion job workflows

"""
import time
from T0.WorkflowFactory.FactoryInterface import FactoryInterface
from T0.RepackConfig.ConvConfigMaker import ConvConfigMaker
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
import ProdCommon.MCPayloads.WorkflowTools as WorkflowTools
import ProdCommon.MCPayloads.DatasetConventions as DatasetConventions

class ConversionFactory(FactoryInterface):
    """
    _ConversionFactory_

    Util to build workflows for accumulator jobs
    
    """
    def __init__(self):
        FactoryInterface.__init__(self)
        self.cmssw = {}
        self.cmssw.setdefault("CMSSWVersion", None)
        self.cmssw.setdefault("CMSPath", None)
        self.cmssw.setdefault("ScramArch", None)
        self.workflow = None

        #  //
        # // Override
        #//  primary, processed and lfn base if needed
        self.primaryDataset = None
        self.processedDataset = None
        self.lfnBase = None
        self.unmergedOutput = False
        self.inputDataset = {}
        self.inputDataset['DatasetName'] = None
        self.inputDataset['Primary'] = None
        self.inputDataset['Processed'] = None
        self.inputDataset['DataTier'] = None

        
        self.run = None
        
    
    def setInput(self, runNumber, datasetPath):
        """
        _setInputDataset_

        """
        datasetBits = DatasetConventions.parseDatasetPath(datasetPath)
        self.inputDataset.update(datasetBits)
        self.inputDataset['DatasetName'] = datasetPath
        self.run = runNumber
        return

    def setCMSSW(self, version, cmsPath, scramArch):
        """
        _setCMSSW_

        Set the CMSSW Version of this workflow that will be used

        """
        self.cmssw['CMSSWVersion'] = version
        self.cmssw['ScramArch'] = scramArch
        self.cmssw['CMSPath'] = cmsPath
        return


    def buildConfiguration(self):
        """
        _buildConfiguration_

        Using a ConvConfigMaker instance, generate a template
        config file

        """
        self.ccm = ConvConfigMaker()

        self.ccm.outputModule['primaryDataset'] = self.primaryDataset
        self.ccm.outputModule['processedDataset'] = self.processedDataset
        self.ccm.outputModule['ParentDataset'] = \
                                           self.inputDataset['DatasetName']
        self.ccm.outputModule["IsUnmerged"] = self.unmergedOutput
        self.ccm.outputModule["LFNBase"] = self.lfnBase

        try:
            self.ccm.createConfig(self.cmssw['CMSSWVersion'],
                                  self.cmssw['CMSPath'],
                                  self.cmssw['ScramArch'])
        except Exception, ex:
            return None
        
        return self.ccm.cfgInterface
        

    def makeWorkflow(self, label):
        """
        _makeWorkflow_

        Create a workflow spec instance for converting the run/dataset
        provided

        """
        #  //
        # // Initialise basic workflow
        #//
        self.label = label
        self.timestamp = int(time.time())
        self.workflow = WorkflowSpec()
        self.workflowName = "%s-%s" % (self.label, "conversion")
        if self.run != None:
            self.workflowName += "-%s" % self.run
        self.workflow.setWorkflowName(self.workflowName)
        self.workflow.setRequestCategory("data")
        self.workflow.setRequestTimestamp(self.timestamp)
        self.workflow.parameters['RequestLabel'] = self.label
        self.workflow.parameters['ProdRequestID'] = self.run
        self.workflow.parameters['RunNumber'] = self.run
        self.workflow.parameters["CMSSWVersion"] = self.cmssw['CMSSWVersion'] 
        self.workflow.parameters['ScramArch'] = self.cmssw['ScramArch'] 
        self.workflow.parameters['CMSPath'] = self.cmssw['CMSPath'] 
        self.cmsRunNode = self.workflow.payload
        self.cmsRunNode.name = "cmsRun1"
        self.cmsRunNode.type = "CMSSW"
        self.cmsRunNode.application['Version'] = self.cmssw['CMSSWVersion']
        self.cmsRunNode.application['Executable'] = "cmsRun"
        self.cmsRunNode.application['Project'] = "CMSSW"
        self.cmsRunNode.application['Architecture'] = self.cmssw['ScramArch']
        
        #  //
        # // Build the configuration template for the workflow
        #//
        self.cmsRunNode.cfgInterface = self.buildConfiguration()
        if self.cmsRunNode.cfgInterface == None:
            return None
        
        for outMod in self.cmsRunNode.cfgInterface.outputModules.keys():
            moduleInstance = self.cmsRunNode.cfgInterface.getOutputModule(
                outMod)
            primaryName = moduleInstance['primaryDataset'] 
            processedName = moduleInstance['processedDataset'] 
            
            outDS = self.cmsRunNode.addOutputDataset(primaryName, 
                                                     processedName,
                                                     outMod)
            outDS['ParenetDataset'] = self.inputDataset['DatasetName']
            outDS['DataTier'] = moduleInstance['dataTier']
            outDS["ApplicationName"] = \
                                     self.cmsRunNode.application["Executable"]
            outDS["ApplicationFamily"] = outMod
            outDS["PhysicsGroup"] = "Tier0"
            outDS["ApplicationFamily"] = outMod
            ###TODO:outDS['PSetHash'] = "What The hell is this now??"

            if moduleInstance['IsUnmerged']:
                outDS['NoMerge'] = True
                
        WorkflowTools.addStageOutNode(self.cmsRunNode, "stageOut1")
        WorkflowTools.addLogArchNode(self.cmsRunNode, "logArchive")
        
    
        
        return self.workflow

if __name__ == '__main__':

    import os
    convF = ConversionFactory()
    convF.primaryDataset = "primary"
    convF.processedDataset = "processed"
    convF.lfnBase = "/store/whatever"
    convF.mergedOutput = False
    convF.setInput(10000001, "/primary/input/RAW")
    
    convF.setCMSSW(os.environ['CMSSW_VERSION'],
                   os.environ['CMS_PATH'],
                   os.environ['SCRAM_ARCH'])
    
    
    wf = convF.makeWorkflow("ConvTest")
    
    wf.save("TestConvWorkflow.xml")

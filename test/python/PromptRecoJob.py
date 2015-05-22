#!/usr/bin/env python2.4

from T0.WorkflowFactory.PromptRecoWorkflow import PromptRecoWorkflow
from T0.JobFactory.PromptRecoJob import PromptRecoJob

newWorkflow = PromptRecoWorkflow()
newWorkflow.setRun("0")
newWorkflow.setStream("STREAM")
newWorkflow.setDataset({"dataset": "dataset", "processedDataset": "processedDataset",
                        "aquisitionEra": "acquisitionEra", "processingVersion": "processingVersion",
                        "globalTag": "globalTag"})
newWorkflow.setCMSSW("CMSSW_2_0_12", "/uscmst1/prod/sw/cms/", "slc4_ia32_gcc345")
        
workflowSpec = newWorkflow.makeWorkflow()
workflowSpec.save("workflow.before.xml")

promptRecoGenerator = PromptRecoJob(workflowSpec)
jobSpec = promptRecoGenerator("0", {"LFN": "LFN"})

workflowSpec.save("workflow.after.xml")

                

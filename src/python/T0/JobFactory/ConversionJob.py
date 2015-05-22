#!/usr/bin/env python
"""
_ConversionJob_

Factory object for generating Conversion JobSpecs from a
conversion workflow spec

"""
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

from T0.JobFactory.FactoryInterface import FactoryInterface
import T0.JobFactory.JobNames as JobNames

from T0.RepackConfig.ConvConfigEditor import ConvConfigEditor

class ConversionJob(FactoryInterface):
    """
    _ConversionJob_

    JobSpec factory for building conversion job specs

    """
    def __init__(self, workflowSpec, count = 0):
        FactoryInterface.__init__(self, workflowSpec)
        self.counter = count
        self.cmssw = {}
        self.cmssw['Version'] = self.workflow.parameters["CMSSWVersion"]
        self.cmssw['ScramArch'] = self.workflow.parameters['ScramArch']
        self.cmssw['CMSPath'] = self.workflow.parameters['CMSPath']
        self.run = self.workflow.parameters['RunNumber']

    def __call__(self,  *inputFiles):
        """
        _operator()_

        Create a new JobSpec to convert the files provided

        """
        
        self.jobSpec = self.workflow.createJobSpec()


        #  //
        # // JobName: Is this vital for conversion jobs??
        #//  Could do something like:
        # jobName = "%s-%s-%s" % (self.workflow.parameters['WorkflowName'],
        #                         self.run, self.counter)
        jobName = JobNames.newConvJobID()
        
        self.jobSpec.setJobName(jobName)
        self.jobSpec.setJobType("Processing")
        self.jobSpec.parameters['RunNumber'] = self.counter
        
        cce = ConvConfigEditor(self.workflow.payload.cfgInterface)
        cce.inputStreamers = list(inputFiles)

        
        self.jobSpec.payload.cfgInterface = cce( self.cmssw['ScramArch'],
                                                 self.cmssw['Version'],
                                                 self.cmssw['CMSPath'] )
        self.counter += 1
        return self.jobSpec


if __name__ == '__main__':
    
    wfspec = '/home/evansde/work/workflows/checkout/T0/src/python/T0/WorkflowFactory/TestConvWorkflow.xml'
    
    factory = ConversionJob(wfspec)
    
    for i in range(0, 10):
        
        streamers = [
            "convertme%s.dat" % (i+x) for x in range(0, 20) ]

        
        spec = factory(*streamers)
        spec.save("JobSpec-%s.xml" % i)

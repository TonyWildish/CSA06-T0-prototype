#!/usr/bin/env python
"""
_injectMCStreamerWorkflow_

Take workflow for generating streamer MC files and

1. Decide what runs and lumi to submit how many jobs for
2. Precreate the RunConfig entries for these runs in T0AST
3. Create Jobs and send them to JobQueue

"""

import os
import sys
import getopt

from MessageService.MessageService import MessageService
from JobQueue.JobQueueAPI import bulkQueueJobs

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.LFNAlgorithm import DefaultLFNMaker
from ProdCommon.CMSConfigTools.ConfigAPI.CfgGenerator import CfgGenerator
from ProdCommon.CMSConfigTools.SeedService import randomSeed

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from WMCore.Configuration import loadConfigurationFile

from T0.GenericTier0 import Tier0DB
from T0.State.Database.Writer import InsertRun
from T0.State.Database.Writer import InsertRunConfig
from T0.RunConfigCache.Tier0Config import retrieveDatasetConfig
from T0.RunConfigCache.Tier0Config import retrieveStreamConfig


class GeneratorMaker(dict):
    """
    _GeneratorMaker_

    Operate on a workflow spec and create a map of node name
    to CfgGenerator instance

    """
    def __init__(self):
        dict.__init__(self)


    def __call__(self, payloadNode):
        if payloadNode.type != "CMSSW":
            return

        if payloadNode.cfgInterface != None:
            generator = CfgGenerator(payloadNode.cfgInterface, False,
                                     payloadNode.applicationControls)
            self[payloadNode.name] = generator
            return

        if payloadNode.configuration in ("", None):
            #  //
            # // Isnt a config file
            #//
            return
        try:
            generator = CfgGenerator(payloadNode.configuration, True,
                                         payloadNode.applicationControls)
            self[payloadNode.name] = generator
        except StandardError, ex:
            #  //
            # // Cant read config file => not a config file
            #//
            return


class JobCreator:
    """
    _JobCreator_

    copy some code from RequestJobFactory

    needs to be wrapped into a class because of the stupid way
    the generateJobConfig and generateCmsGenConfig methods work
    (really do not want to take this apart)
    """
    def __init__(self, workflowSpec, **args):
        self.run = None
        self.lumi = None
        self.jobName = None
        self.eventsPerJob = None
        self.generators = GeneratorMaker()
        workflowSpec.payload.operate(self.generators)

    def setRun(self, run):
        self.run = run

    def setLumi(self, lumi):
        self.lumi = lumi

    def setJobName(self, jobName):
        self.jobName = jobName

    def setEventsPerJob(self, eventsPerJob):
        self.eventsPerJob = eventsPerJob

    def setFirstEvent(self, firstEvent):
        self.firstEvent = firstEvent

    def generateJobConfig(self, jobSpecNode):
        """
        _generateJobConfig_

        Operator to act on a JobSpecNode tree to convert the template
        config file into a JobSpecific Config File

        """
        if jobSpecNode.name not in self.generators.keys():
            return
        generator = self.generators[jobSpecNode.name]

        jobCfg = generator(
            self.jobName,
            maxEventsWritten = self.eventsPerJob,
            firstEvent = self.firstEvent,
            firstRun = self.run,
            firstLumi = self.lumi)

        jobSpecNode.cfgInterface = jobCfg
        return

    def generateCmsGenConfig(self, jobSpecNode):
        """
        _generateCmsGenConfig_

        Process CmsGen type nodes to insert maxEvents and run numbers
        for cmsGen jobs

        """
        if jobSpecNode.type != "CmsGen":
            return

        # this is intentional !!!
        jobSpecNode.applicationControls['firstRun'] = self.lumi

        jobSpecNode.applicationControls['maxEvents'] = self.eventsPerJob
        jobSpecNode.applicationControls['randomSeed'] = randomSeed()
        jobSpecNode.applicationControls['fileName'] = "%s-%s.root" % (
            self.jobName, jobSpecNode.name)
        jobSpecNode.applicationControls['logicalFileName'] = "%s-%s.root" % (
            self.jobName, jobSpecNode.name)
        return



valid = ['workflow=',
         'firstrun=',
         'lastrun=',
         'lumiperrun=',
         'jobsperlumi=',
         'eventsperjob=',
         ]

usage = "Usage: injectMCStreamerWorkflow.py --worklflow=<workflowFile>\n"
usage += "                                  --firstrun=<first run>\n"
usage += "                                  --lastrunl=<lastrun>\n"
usage += "                                  --lumiperrun=<lumi sections per run>\n"
usage += "                                  --jobsperlumi=<#jobs with same lumi section>\n"
usage += "                                  --eventsperjob=<#events per job>\n"
usage += "\n"

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

workflow=None
firstrun=None
lastrun=None
lumiperrun=1
jobsperlumi=1
eventsperjob=1

for opt, arg in opts:
    if opt == "--workflow":
        workflow = arg
    if opt == "--firstrun":
        firstrun = int(arg)
    if opt == "--lastrun":
        lastrun = int(arg)
    if opt == "--lumiperrun":
        lumiperrun = int(arg)
    if opt == "--jobsperlumi":
        jobsperlumi = int(arg)
    if opt == "--eventsperjob":
        eventsperjob = int(arg)

if workflow == None:
    msg = "--workflow option not provided: This is required"
    raise RuntimeError, msg
if firstrun == None:
    msg = "--firstrun option not provided: This is required"
    raise RuntimeError, msg
if lastrun == None:
    msg = "--lastrun option not provided: This is required"
    raise RuntimeError, msg


# Load the workflow spec and ensure it has the TotalEvents and
# EventsPerJob settings in the parameters

if not os.path.exists(workflow):
    raise RuntimeError, "Workflow not found: %s" % workflow

workflowSpec = WorkflowSpec()
try:
    workflowSpec.load(workflow)
except Exception, ex:
    msg = "Unable to read workflow file:\n%s\n" % workflow
    msg += str(ex)
    raise RuntimeError, msg

ms = MessageService()
ms.registerAs("injectMCStreamerWorkflow")
ms.publish("NewWorkflow", workflow)
ms.commit()

#
# Load PA and T0 configs
#
paConfig = loadProdAgentConfiguration()
tier0Config = loadConfigurationFile(paConfig.getConfig("RunConfig")["OfflineConfDB"])

#
# Connect to T0AST
#

t0astDBConfig = paConfig.getConfig("Tier0DB")
t0astDBConn = Tier0DB.Tier0DB(t0astDBConfig,
                              manageGlobal = True)
t0astDBConn.connect()

#
# fix versions
#

onlineVersion = "CMSSW_2_2_13"
repackVersion = tier0Config.Global.RepackVersionMappings.get(onlineVersion, onlineVersion)
expressVersion = tier0Config.Global.ExpressVersionMappings.get(onlineVersion, onlineVersion)
hltkey = "/fake/hlt/key"

processName = "HLT"
mapping = { 'Tier0MCFeeder1' : { 'Cosmics' : [ 'path1' ] },
            'Tier0MCFeeder2' : { 'Calo' : [ 'path2' ] } }

for run in range(firstrun, lastrun+1):

    InsertRun.insertRun(t0astDBConn, run,
                        hltkey = hltkey,
                        runversion = onlineVersion,
                        repackversion = repackVersion,
                        expressversion = expressVersion)

    for lumi in range(1,lumiperrun+1):
        InsertRun.insertLumiSection(t0astDBConn, run, lumi)

    InsertRunConfig.insertRunConfig(t0astDBConn, run,
                                    processName, mapping,
                                    tier0Config.Global.AcquisitionEra)

    InsertRunConfig.insertT0Config(t0astDBConn, run,
                                   tier0Config.Global.Version)

    for streamName in mapping.keys():
        streamConfig = retrieveStreamConfig(tier0Config, streamName)
        InsertRunConfig.insertStreamConfig(t0astDBConn, run,
                                           streamName, streamConfig)

        for datasetName in mapping[streamName]:
            datasetConfig = retrieveDatasetConfig(tier0Config, datasetName)
            InsertRunConfig.insertDatasetConfig(t0astDBConn, run,
                                                datasetConfig)

t0astDBConn.commit()

jobCreator = JobCreator(workflowSpec)

for run in range(firstrun, lastrun+1):

    jobCreator.setRun(run)

    # if this is needed we should create
    # a JobCreator instance per run
    #workflowSpec.setWorkflowRunNumber(run)

    jobList = []
    for lumi in range(1,lumiperrun+1):

        jobCreator.setLumi(lumi)
        jobCreator.setEventsPerJob(eventsperjob)
        jobCreator.setFirstEvent(1+lumi*eventsperjob)

        jobName = "%s-%s-%s" % (workflowSpec.workflowName(),
                                run, lumi)

        jobSpec = workflowSpec.createJobSpec()

        jobSpecDir =  os.path.join("/data/hufnagel/parepack/StreamerMCRunning",
                                   str(run // 1000).zfill(4))
        if not os.path.exists(jobSpecDir):
            os.makedirs(jobSpecDir)

        jobSpecFileName = jobName + "-jobspec.xml"
        jobSpecFile = os.path.join(jobSpecDir, jobSpecFileName) 

        jobSpec.setJobName(jobName)

        # used for thresholds
        jobSpec.setJobType("Processing")

        # this sets lumi section !!!
        jobSpec.parameters['RunNumber'] = lumi
        
        jobSpec.parameters['JobSpecFile'] = jobSpecFile

        jobCreator.setJobName(jobName)
        #jobSpec.payload.operate(DefaultLFNMaker(jobSpec))
        jobSpec.payload.operate(jobCreator.generateJobConfig)
        #jobSpec.payload.operate(jobCreator.generateCmsGenConfig)

        jobSpec.save(jobSpecFile)

        jobDict = {
            "JobSpecId" : jobName,
            "JobSpecFile": jobSpecFile,
            "JobType" : "Processing",
            "WorkflowSpecId" : workflowSpec.workflowName(),
            "WorkflowPriority" : 10,
            }
        jobList.append(jobDict)

    bulkQueueJobs([], *jobList)

    print "Queued %d jobs for run %s" % (len(jobList),run)

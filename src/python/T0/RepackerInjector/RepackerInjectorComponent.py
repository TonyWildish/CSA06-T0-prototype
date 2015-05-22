#!/usr/bin/env python
"""
_RepackerInjectorComponent_

Component for generating Repacker jobsspecs
"""

__version__ = "$Revision: 1.70 $"
__revision__ = "$Id: RepackerInjectorComponent.py,v 1.70 2009/04/14 19:07:25 sfoulkes Exp $"

import os
import shutil
import logging

from JobQueue import JobQueueDB

from T0.GenericTier0.GenericTier0Component import GenericTier0Component

from T0.WorkflowFactory.RepackWorkflow import RepackWorkflow
from T0.JobFactory.StreamerJob import StreamerJob

from T0.RepackerInjector.RepackerInjectorAPI import getJobs
from T0.RepackerInjector.RepackerInjectorAPI import updateJobs


class RepackerInjectorComponent(GenericTier0Component):
    """
    _RepackerInjectorComponent_

    Component for generating Repacker jobspecs.
    """
    def __init__(self, **args):
        GenericTier0Component.__init__(self, **args) 
        
        self.cmsswVer = self.lookupParameter("cmsswVer", "CMSSW_2_0_3")
        self.scramArch = self.lookupParameter("scramArch", "slc4_ia32_gcc345")
        self.cmsPath = self.lookupParameter("cmsPath", "/uscmst1/prod/sw/cms/")
        self.pollInterval = self.lookupParameter("PollInterval", "00:01:00")
        self.jobCacheDir = self.lookupParameter("JobCacheDir",
                                                self.args["ComponentDir"])
        self.workflowSpecQueue = {}

        if self.lookupParameter("UseLazyDownload", "Off") == "On":
            self.useLazyDownload = True
        else:
            self.useLazyDownload = False
        
        logging.info("RepackerInjector Component Started")


    def __call__(self, message, payload):
        """
        _operator(message, payload)_

        Method called in response to this component receiving a message

        """
        # always have a debug on/off switch
        if message == "RepackerInjector:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        
        if message == "RepackerInjector:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        #  //
        # // Component Specific actions.
        #//
        
        if message == "RepackerInjector:Poll":
            self.poll()
            return
        
        if message == "CleanUp":
            self.cleanupRun(payload)
            return


    def cleanupRun(self, runNumber):
        """
        _cleanupRun_

        Remove all workflows and jobspecs for a particular run from disk.
        Also, remove any workflows for the run from memory.
        """
        try:
            runNumber = int(runNumber)
        except ValueError:
            logging.error("Run number is not valid: %s" % runNumber)
            return

        logging.debug("Cleaning up run %s" % runNumber)
        
        # remove workflow from queue
        if runNumber in self.workflowSpecQueue.keys():
            del self.workflowSpecQueue[runNumber]
            
        workflowDir = self.jobCacheDir + "/workflows/Run%s" % runNumber
        jobSpecDir = self.jobCacheDir + "/jobspecs/Run%s" % runNumber
        
        shutil.rmtree(workflowDir,ignore_errors=True)
        shutil.rmtree(jobSpecDir,ignore_errors=True)


    def createNewWorkflow(self, runNumber, stream_id, cmsswVer):
        """
        create the workflow template and publish message for a given run

        """
        # no need to check return value, already did this earlier
        runConfig = self.getRunConfig(runNumber)

        stream = runConfig.getStreamName(stream_id)

        logging.debug("Create new workflow for run %d and stream %s" % (runNumber, stream))

        outModuleInfoList = runConfig.getOutputModuleInfoList("Repacker", stream_id, "RAW",
                                                              splitIntoDatasets = True)
        if outModuleInfoList == None:
            logging.error("No output module list for run %s and stream %s" % (runNumber, stream))
            return None

        workflow = RepackWorkflow(runNumber,
                                  cmsswVer,
                                  self.cmsPath,
                                  self.scramArch,
                                  *outModuleInfoList)

        workflowSpec = workflow.makeWorkflow("Repack-Run%s-Stream%s" % (runNumber, stream),
                                             self.useLazyDownload)

        if workflowSpec == None:
            return None

        workflowDir = os.path.join(self.jobCacheDir, "workflows",
                                   "Run%s" % runNumber)
        if not os.path.isdir(workflowDir):
            os.makedirs(workflowDir)

        workflowSpecFile = os.path.join(workflowDir,
                                        "Repack-Run%s-Stream%s-workflow.xml" % (runNumber, stream))
        workflowSpec.save(workflowSpecFile)

        logging.debug("Workflow file saved as %s" % workflowSpecFile)

        self.ms.publish("NewWorkflow", workflowSpecFile)
        self.ms.commit()

        return workflowSpec


    def createJobs(self):
        """
        _createJobs_

        main routine called in polling cycle

        """
        # TODO: handle exception (which ones, where do they come from)
        jobEntityList = getJobs(self.t0astDBConn)
        
        # sort by run and stream_id
        jobEntityDict = {}
        for jobEntity in jobEntityList:
            run = jobEntity["runNumber"]
            stream_id = jobEntity["streamID"]
            if jobEntityDict.has_key(run):
                if jobEntityDict[run].has_key(stream_id):
                    jobEntityDict[run][stream_id].append(jobEntity)
                else:
                    jobEntityDict[run][stream_id] = [ jobEntity ]
            else:
                jobEntityDict[run] = {}
                jobEntityDict[run][stream_id] = [ jobEntity ]

        # now process by run and stream_id
        # only try to create new workflow once per run and stream_id
        # keep track of which jobs were created
        createdJobs = []
        createdJobSpecs = []
        for run in sorted(jobEntityDict.keys()):

            #
            # check that this run has a RunConfig record
            #
            runConfig = self.getRunConfig(run)

            if runConfig == None:
                logging.error("Could not retrieve run config for run %s" % run)
                continue

            #
            # retrieve CMSSW version for repacking (not stream dependent)
            #
            cmsswVer = runConfig.repackCMSSWVersion()
            if cmsswVer == None:
                logging.error("No repack framework version for run %s" % run)
                continue

            jobSpecBaseDir = os.path.join(self.jobCacheDir,
                                          "jobspecs",
                                          "Run%d" % jobEntity["runNumber"])

            for stream_id in sorted(jobEntityDict[run].keys()):

                workflowSpecsByRun = self.workflowSpecQueue.get(run, None)
                if workflowSpecsByRun == None:
                    workflowSpecsByRun = {}
                    self.workflowSpecQueue[run] = workflowSpecsByRun

                workflowSpec = workflowSpecsByRun.get(stream_id, None)

                if workflowSpec == None:
                    workflowSpec = self.createNewWorkflow(run, stream_id, cmsswVer)
                    if workflowSpec == None:
                        logging.error("Can't create workflow for run %d and stream %s"
                                      % (run, runConfig.getStreamName(stream_id)))
                        continue
                    else:
                        self.workflowSpecQueue[run][stream_id] = workflowSpec

                # reuse job factory for all jobs in a run and stream
                jobFactory = StreamerJob(workflowSpec)
                for jobEntity in jobEntityDict[run][stream_id]:
                    jobSpec = jobFactory("Repack", jobEntity, jobSpecBaseDir)
                    createdJobs.append(jobEntity)
                    createdJobSpecs.append(jobSpec)

        if len(createdJobs) > 0:

            try:
                updateJobs(self.t0astDBConn, createdJobs)

                for jobSpec in createdJobSpecs:
                    JobQueueDB.insertJobSpec(self.paDBConn.getDBInterface(),
                                             jobSpec.parameters["JobName"],
                                             jobSpec.parameters['JobSpecFile'],
                                             jobSpec.parameters["JobType"],
                                             jobSpec.payload.workflow, 1)

                self.paDBConn.commit()
                self.t0astDBConn.commit()
                logging.info("Created and queued %d new jobs" % len(createdJobs))
            except StandardError, ex:
                # close and rethrow
                self.t0astDBConn.rollback()
                self.paDBConn.rollback()
                logging.error("Failed to create jobs : %s\n" % str(ex))

        return

    
    def poll(self):
        """
        _poll_

        Single cycle invocation of job creation

        """
        logging.info("Job creation poll invoked")
        
        self.createJobs()
        
        #  //
        # // Lastly, delay the poll cycle every time we poll, since
        #//  we only want it as a catch all/timeout, so we remove the existing
        #  //poll and replace it with another full interval
        # //
        #//
        self.ms.publishUnique("RepackerInjector:Poll", "", self.pollInterval)
        self.ms.commit()

        return
    
    def startComponent(self):
        """
        _startComponent_

        Start up the component and define the messages that it subscribes to.
        """
        self.ms.subscribeTo("RepackerInjector:StartDebug")
        self.ms.subscribeTo("RepackerInjector:EndDebug")
        self.ms.subscribeTo("RepackerInjector:Poll")

        # published by JobScheduler Component
        self.ms.subscribeTo("NewRepackJobs")
        self.ms.subscribeTo("CleanUp")
        
        self.connectT0AST()
        self.connectPADB()
        
        self.ms.publishUnique("RepackerInjector:Poll", "", "00:01:00")
        self.ms.commit()
        
        # wait for messages
        self.messageLoop()

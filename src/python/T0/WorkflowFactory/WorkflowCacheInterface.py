#!/usr/bin/env python
"""
_WorkflowContainerInterface_

Interface for a workflow container class that caches the most recently
generated/accessed workflows.
"""

__revision__ = "$Id: WorkflowCacheInterface.py,v 1.1 2009/04/08 20:05:26 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import time
import logging
import os

class WorkflowCacheInterface(dict):
    """
    _WorkflowCacheInterface_

    Interface for a workflow container class that caches the most recently
    generated/accessed workflows.
    """
    def __init__(self, messagingSystem, dbConn, cacheDir, **params):
        """
        ___init___

        Setup some useful defaults and also store the location of the
        workflow cache.
        """
        self.workflowCacheDir = cacheDir
        self.ms = messagingSystem
        self.t0astDBConn = dbConn
        self.update(params)

        self.maxWorkflowsInCache = 3

        # The following two data structures hold the generated workflows and the
        # access times of the workflows so we can prune out old workflows.
        self.workflows = {}
        self.accessTimes = {}
        return

    def pruneOldWorkflows(self):
        """
        _pruneOldWorkflows_

        Check the workflow cache to see how many workflows it contains.  If
        there are too many workflows, remove the oldset one.
        """
        oldestEntryTime = [time.time()]
        oldestEntryKey = None
        
        totalWorkflows = len(self.workflows.keys())
        if totalWorkflows < self.maxWorkflowsInCache:
            return

        for workflow in self.workflows.keys():
            if oldestEntryKey == None:
                oldestEntryKey = workflow
                oldestEntryTime = self.accessTimes[workflow]
            else:
                if oldestEntryTime > self.accessTimes[workflow]:
                    oldestEntryKey = workflow
                    oldestEntryTime = self.accessTimes[workflow]                    

        del self.workflows[oldestEntryKey]
        del self.accessTimes[oldestEntryKey]            
        return

    def retrieveWorkflow(self, runConfig, **params):
        """
        _retrieveWorkflow_

        Retrieve a workflow for a particular run and dataset.  The runConfig
        object must correspond to the run of the workflow being retrieved.
        """
        self.pruneOldWorkflows()

        runNumber = runConfig.getRunNumber()
        workflowKey = self.generateKey(runConfig, params)

        if workflowKey not in self.workflows.keys():
            self.workflows[workflowKey] = None

        self.accessTimes[workflowKey] = time.time()
        existingWorkflow = self.workflows[workflowKey]

        if existingWorkflow != None:
            return existingWorkflow

        newWorkflow = self.createWorkflow(runConfig, params)
        self.workflows[workflowKey] = newWorkflow
        return newWorkflow

    def createWorkflow(self, runConfig, params):
        """
        _createWorkflow_

        Create a workflow.  Return the workflow to the calling function.
        """
        pass

    def generateKey(self, runConfig, params):
        """
        _generateKey_

        Given a set of input parameters generate a key that will be unique to
        the workflow and can be used to identify the workflow.
        """
        pass

    def saveWorkflow(self, workflowSpec):
        """
        _saveWorkflow_

        Save a workflow spec to disk also publish "NewWorkflow" and "NewDataset"
        messages.
        """
        workflowDir = os.path.join(self.workflowCacheDir,
                                   "Run%d" % workflowSpec.workflowRunNumber())
        if not os.path.isdir(workflowDir):
            os.makedirs(workflowDir)

        workflowPath = "%s/%s-workflow.xml" % (workflowDir,
                                               workflowSpec.workflowName())
        workflowSpec.save(workflowPath)

        self.ms.publish("NewWorkflow", workflowPath)
        self.ms.publish("NewDataset", workflowPath)
        self.ms.commit()
        return

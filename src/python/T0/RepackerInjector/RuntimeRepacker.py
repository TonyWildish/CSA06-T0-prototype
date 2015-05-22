#!/usr/bin/env python
"""
_RuntimeRepacker_

Runtime Script for unpacking a repacker job configuration

"""

import os
import pickle

from ProdCommon.FwkJobRep.TaskState import TaskState
from ProdCommon.MCPayloads.JobSpec import JobSpec
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec


class NodeFinder:
    def __init__(self, nodeName):
        self.nodeName = nodeName
        self.result = None

    def __call__(self, nodeInstance):
        if nodeInstance.name == self.nodeName:
            self.result = nodeInstance

def sortByValue(d):
    """ Returns the keys of dictionary d sorted by their values """
    items = d.items()
    backitems = [ [v[1], v[0]] for v in items]
    backitems.sort()
    return [ backitems[i][1] for i in range(0, len(backitems))]


class RepackerSetup:
    """
    _RepackerSetup_

    Object to manipulate the Configuration files for a repacker job

    - Extract the details of the repacker job entity stored in the
      config

    - Pull in the lumi server information and add it to the config

    """
    def __init__(self, workflowSpec, jobSpec):

        self.workflowSpec = WorkflowSpec()
        self.workflowSpec.load(workflowSpec)

        self.jobSpec = JobSpec()
        self.jobSpec.load(jobSpec)

        taskState = TaskState(os.getcwd())
        taskState.loadRunResDB()

        jobSpecFinder = NodeFinder(taskState.taskName())
        self.jobSpec.payload.operate(jobSpecFinder)
        self.jobSpecNode = jobSpecFinder.result

        workflowFinder = NodeFinder(taskState.taskName())
        self.workflowSpec.payload.operate(workflowFinder)
        self.workflowNode = workflowFinder.result

        self.run = None
        self.lumis = []
        self.streamerFiles = []
        self.activeDatasets = []


    def unpackJobEntity(self):
        """
        _unpackJobEntity_

        Get the StreamerJobEntity from the JobSpec node

        """

        repackJobEntity = self.jobSpecNode.cfgInterface.extensions.get('Streamer', None)
        if repackJobEntity == None:
            msg = "No StreamerJobEntity in JobSpec configuration\n"
            msg += "This is required for repacker jobs\n"
            raise RuntimeError, msg

        # Get run and lumi numbers for this job
        self.run = repackJobEntity.data['runNumber']
        self.lumis = repackJobEntity.data['lumiSections']
        print "Repacker Job Handling Run:%s\n LumiSections: %s\n" % (self.run,self.lumis)

        # Sort streamer input by lumi ID for time ordering
        self.streamerFiles = sortByValue(repackJobEntity.data['streamerFiles'])
        msg = "Streamer Files for this job are:\n"
        for strmr in self.streamerFiles:
            msg += "  %s\n" % strmr
        print msg

        # Get list of active datasets for this job
##        self.activeDatasets = repackJobEntity.data['activeOutputModules']
##        msg = "This Job Will repack datasets:\n"
##        for dataset in self.activeDatasets:
##            msg += "  %s\n" % dataset
##        print msg

        return


    def backupPSet(self,filename,process):
        """
        _backupPSet_
        
        Write a backup copy of the current PSet to disk.
        """
        print "Wrote current configurations as %s" % filename
        handle = open(filename, 'w')
        handle.write("import pickle\n")
        handle.write("pickledCfg=\"\"\"%s\"\"\"\n" % pickle.dumps(process))
        handle.write("process = pickle.loads(pickledCfg)\n")
        handle.close()

        return


    def importAndBackupProcess(self):
        """
        _importAndBackupProcess_
        
        Try to import the process object for the job, which is contained in
        PSet.py and save a backup copy of it.
        """
        try:
            from PSet import process
        except ImportError, ex:
            msg = "Failed to import PSet module containing cmsRun Config\n"
            msg += str(ex)
            raise RuntimeError, msg

        print "PSet.py imported"

        self.backupPSet("PSetPreRepack.log",process)

        return process


    def dumpConfiguration(self,process):
        """
        _dumpConfiguration_
        
        Write out the final configuration used for the job.
        """

        cmsswVersion = self.workflowSpec.parameters.get("CMSSWVersion")
        cmsswVersionList = cmsswVersion.split('_')

        if ( cmsswVersionList[1] >= '2' and cmsswVersionList[2] >= '1' ):
            pycfgDump = open("PyCfgFileDump.log", 'w')
            try:
                print "Writing out PyCfgFileDump.log"
                pycfgDump.write(process.dumpPython())
            except Exception, ex:
                msg = "Error writing Python format config dump:\n"
                msg += "%s\n" % str(ex)
                msg += "This needs to be reported to the framework team"
                pycfgDump.write(msg)
            pycfgDump.close()
        else:
            cfgDump = open("CfgFileDump.log", 'w')
            try:
                print "Writing out CfgFileDump.log"
                cfgDump.write(process.dumpConfig())
            except Exception, ex:
                msg = "Error writing CfgFile format config dump\n"
                msg += "%s\n" % str(ex)
                msg += "This needs to be reported to the framework team"
                cfgDump.write(msg)
            cfgDump.close()

        return


    def importPSet(self):
        """
        _importPSet_

        Import the process object for cmsRun

        """

        process = self.importAndBackupProcess()

        #  //
        # // Insert input files and remove unused output modules
        #//  Then dump the config for logging purposes
        process.source.fileNames = self.streamerFiles
        self.pruneOutputModules(process)
        print "Streamers inserted and output modules pruned"

##        self.backupPSet("PSetPreLumi.log",process)

##        #  //
##        # // Insert LumiServer information
##        #//  Then dump the config for logging purposes
##        try:
##            print "Inserting Lumi Data..."
##            self.insertLumiInfo(process)

##            print "Inserted Lumi Data OK"
##        except Exception, ex:
##            msg = "Error inserting Lumi Data:\n"
##            msg += str(ex)
##            msg += "\n"
##            print msg

##            #  //
##            # // TODO: Is this terminal for the job??
##            #//  Probably indicates a wider problem...
##            return

        # Dump and save final configuration
        self.dumpConfiguration(process)
        self.backupPSet("PSet.py",process)

        return


##    def insertLumiInfo(self, process):
##        """
##        _insertLumiInfo_

##        Insert Luminosity Server information into the config

##        """
##        from T0.LumiData.InsertLumi import insertLumi

##        insertLumi(self.run, self.lumis, process)

##        return


    def pruneOutputModules(self, process):
        """
        _pruneOutputModules_

        For the given process, traverse all output modules
        and remove those that do not contain a stream
        parameter in the listOfStreams provided

        """
        from FWCore.ParameterSet.Config import EndPath

##        toPrune = []
        for outputModuleName,outputModule in process.outputModules.iteritems():

##            dataset = getattr(outputModule, "dataset", None)

##            if dataset == None:
##                toPrune.append(outputModuleName)
##                continue

##            primaryDataset = getattr(dataset, "primaryDataset", None)

##            if primaryDataset == None or primaryDataset.value() not in self.activeDatasets:
##                toPrune.append(outputModuleName)
##                continue

            # Keeping module => add it to the endpath
            endPath = getattr(process, "outputPath", None)
            if endPath == None:
                process.outputPath = EndPath(outputModule)
            else:
                process.outputPath += outputModule

##        # Pruning dead modules
##        for deadModule in toPrune:
##            del process._Process__outputmodules[deadModule]

        # Interesting fact, the dead modules are removed from
        # the configuration, but they still show up with their
        # names in the pickled configuration
        #
        # Not sure why, but they are gone after unpickling,
        # so everything still works

        return


def main():
    """
    _main_

    Verify that the environment variable that points at the workflow spec
    has been set correctly and then make modification to the process object

    """
    print "=========Repacker Job Setup================="

    workflowSpec = os.environ.get("PRODAGENT_WORKFLOW_SPEC", None)
    if workflowSpec == None:
        msg = "Unable to find WorkflowSpec from "
        msg += "PRODAGENT_WORKFLOW_SPEC variable\n"
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    if not os.path.exists(workflowSpec):
        msg = "Cannot find WorkflowSpec file:\n %s\n" % workflowSpec
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    jobSpec = os.environ.get("PRODAGENT_JOBSPEC", None)
    if jobSpec == None:
        msg = "Unable to find JobSpec from PRODAGENT_JOBSPEC variable\n"
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    if not os.path.exists(jobSpec):
        msg = "Cannot find WorkflowSpec file:\n %s\n" % jobSpec
        msg += "Unable to proceed\n"
        raise RuntimeError, msg

    instance = RepackerSetup(workflowSpec,jobSpec)
    instance.unpackJobEntity()
    instance.importPSet()

    print "=========Repacker Job Setup Done==========="

if __name__ == '__main__':
    main()

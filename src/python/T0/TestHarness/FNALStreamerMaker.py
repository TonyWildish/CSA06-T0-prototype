#!/usr/bin/env python
"""
_FNALStreamerMaker_

Util to create a set of test files at FNAL.

Generates a set of config files and a main job script to
run the jobs in the FNAL condor queue and stage out the
data

"""

import os
import sys
import random
from T0.TestHarness.StreamerConfig import generateRun
from T0.TestHarness.StreamerConfig import uuidgen
import pickle


class StreamerGenerator:
    """
    _StreamerGenerator_

    Util to generate a set of batch jobs and condor JDL file
    to run the jobs at FNAL for testing

    """
    def __init__(self):
        self.workdir = os.getcwd()
        self.version = os.environ['CMSSW_VERSION'],
        self.lfnBase = "/store/data/repacker-tests/%s-tests" % self.version
        self.run = 100000
        self.lumiPerRun = 2
        self.filesPerLumi = 8
        self.eventsPerFile = 2290
        self.triggerDefs = {}
        
        [self.triggerDefs.__setitem__("p%s" % i, abs(random.gauss(0.1, 0.1)))
         for i in range(1, 51) ]

        self.runScript = [
            "#!/bin/bash",
            "export JOB_INITIALDIR=`pwd`",
            ". $OSG_GRID/setup.sh",
            ". $OSG_APP/cmssoft/cms/cmsset_default.sh",
            ". /uscmst1/prod/sw/cms/setup/bashrc prod",
            "(",
            "export SCRAM_ARCH=slc4_ia32_gcc345",
            "scramv1 project CMSSW %s" % self.version,
            "cd %s" % self.version,
            "eval `scramv1 ru -sh`",
            "cmsRun $JOB_INITIALDIR/$STREAMER_INPUT_CONFIG",
            "edmStreamerIndex -i streamer-out.ind -o $STREAMERINDX_OUTPUT_XML",
            "cat $STREAMERINDX_OUTPUT_XML",
            "cp ./$STREAMERINDX_OUTPUT_XML $JOB_INITIALDIR",
            ")",
            "/bin/ls",
            "/bin/ls %s" % self.version, 
            "srmcp file:///`pwd`/%s/streamer-out.dat srm://cmssrm.fnal.gov:8443/srm/managerv1?SFN=/11$STREAMER_OUTPUT_LFN" % self.version,
            "srmcp file:///`pwd`/%s/streamer-out.ind srm://cmssrm.fnal.gov:8443/srm/managerv1?SFN=/11$STREAMERINDX_OUTPUT_LFN" % self.version,
            ]
        
    def __call__(self):

        #  //
        # // Keep record of triggers
        #//
        trigXML = os.path.join(self.workdir, "TriggerTable.xml")
        handle = open(trigXML, 'w')
        handle.write("<TriggerPaths Run=\"%s\">\n" % self.run)
        for key, val in self.triggerDefs.items():
            handle.write("  <TriggerPath Name=\"%s\"/>\n" % key)
        handle.write("</TriggerPaths>\n")
        handle.close()
        
        #  //
        # // make main script
        #//
        mainScript = os.path.join(self.workdir, "makeStreamer.sh")
        handle = open(mainScript, 'w')
        for line in self.runScript:
            handle.write("%s\n" % line)
        handle.close()

        configs = generateRun(self.run,
                              self.lumiPerRun, self.filesPerLumi,
                              self.eventsPerFile,
                              **self.triggerDefs)
        configFiles = {}
        configCounter = 0
        for conf in configs:
            confName = "streamerconf-%s.py" % configCounter
            confFile = os.path.join(
                self.workdir, confName)
            handle = open(confFile, 'w')
            handle.write("import pickle\n")
            handle.write("pickledCfg=\"\"\"%s\"\"\"\n" % pickle.dumps(conf))
            handle.write("process = pickle.loads(pickledCfg)\n")
            handle.close()
            configFiles[confName] =confFile
            configCounter +=1

        jdlFile = os.path.join(self.workdir, "submit.jdl")
        
        jdl = []
        jdl.append("universe = globus\n")
        jdl.append("globusscheduler = cmsosgce.fnal.gov/jobmanager-condor\n")
        jdl.append("initialdir = %s\n" % self.workdir)
        jdl.append("Executable = %s\n" % mainScript)
        jdl.append("notification = NEVER\n")
        for job in configFiles.keys():
            guid = uuidgen()
            strmrLfn = "%s/%s-streamer.dat" % (self.lfnBase, guid)
            indxLfn = "%s/%s-streamer.ind" % (self.lfnBase, guid)
            indxXML = "%s-index.xml" % job
            
            jdl.append("transfer_input_files = %s\n" % configFiles[job])
            jdl.append("should_transfer_files = YES\n")
            jdl.append("when_to_transfer_output = ON_EXIT\n")
            jdl.append("transfer_output_files = %s\n" % indxXML)
            jdl.append("Output = %s-condor.out\n" % job)
            jdl.append("Error = %s-condor.err\n" %  job)
            jdl.append("Log = %s-condor.log\n" % job)
            envSettings = "environment = STREAMER_INPUT_CONFIG=%s;STREAMER_OUTPUT_LFN=%s;STREAMERINDX_OUTPUT_LFN=%s;STREAMERINDX_OUTPUT_XML=%s \n" % (
                job, strmrLfn, indxLfn, indxXML
                )
            jdl.append(envSettings)            
            jdl.append("Queue\n")

        handle = open(jdlFile, 'w')
        handle.writelines(jdl)
        handle.close()


        

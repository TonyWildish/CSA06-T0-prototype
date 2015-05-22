#!/usr/bin/env python
"""
_StreamerConfig_

Utilities for generating fake streamer files

"""
import time
import random
import os
import pickle
import popen2
from random import SystemRandom
_inst  = SystemRandom()
_MAXINT = 900000000

from FWCore.ParameterSet.Config import Process, Path, EndPath
from FWCore.ParameterSet.Modules import Source, OutputModule, EDProducer, Service, EDFilter
import FWCore.ParameterSet.Types as CmsTypes


def timestamp():
    return time.strftime("%d-%b-%Y-%H:%M:%S")

def seed():
    try:
        value =  _inst.randint(1, _MAXINT)
    except:
        value =  random.randint(1, _MAXINT)
    return value

    
    return

def uuidgen():
    """
    Try to create a uuid with uuidgen if available, returns None if not
    """
    pop = popen2.Popen4("uuidgen")
    pop.wait()
    exitCode = pop.poll()
    if exitCode:
        return None
    hashVal = pop.fromchild.read().strip()
    return hashVal

def makeProcess(numEvents = 200):
    """
    _makeProcess_

    Create a new Process instance

    """
    
    proc = Process("HLT")
    proc.include("FWCore/MessageLogger/data/MessageLogger.cfi")
    


    configName =  "fake-streamer-config"
    configVersion = timestamp()
    configAnnot =  "auto generated fake streamer  config"

    proc.configurationMetadata = CmsTypes.untracked(CmsTypes.PSet())
    proc.configurationMetadata.name = CmsTypes.untracked(CmsTypes.string(
        configName))
    proc.configurationMetadata.version = CmsTypes.untracked(CmsTypes.string(
        configVersion))
        
    proc.configurationMetadata.annotation = CmsTypes.untracked(CmsTypes.string(
        configAnnot))

    
    proc.options = CmsTypes.untracked(CmsTypes.PSet())
    proc.options.wantSummary = CmsTypes.untracked(CmsTypes.bool(True))

    proc.source = Source("EmptySource")

    proc.maxEvents = CmsTypes.untracked(CmsTypes.PSet())
    proc.maxEvents.input = CmsTypes.untracked(CmsTypes.int32(numEvents))
    

    proc.prod = EDProducer("StreamThingProducer")
    proc.prod.array_size = CmsTypes.int32(2500)
    proc.prod.instance_count = CmsTypes.int32(150)
    proc.prod.apply_bit_mask = CmsTypes.untracked(CmsTypes.bool(True))
    proc.prod.bit_mask = CmsTypes.untracked( CmsTypes.uint32( 16777215))

    proc.add_(Service("RandomNumberGeneratorService"))
    
    svc = proc.services["RandomNumberGeneratorService"]
    svc.moduleSeeds = CmsTypes.PSet()
    
    proc.makeData = Path(proc.prod)
    
    return proc


def makeTriggerEntry(process, triggerPath, selectionEff):
    """
    _makeTriggerEntry_

    Add a single trigger entry to the configuration.

    This adds:
    - Random seed
    - 

    """
    modName = "mod%s" % triggerPath
    
    svc = process.services["RandomNumberGeneratorService"]
    setattr(svc.moduleSeeds, modName, CmsTypes.untracked( CmsTypes.uint32(
        seed() )))
    
    filterMod = EDFilter("RandomFilter")
    filterMod.acceptRate = CmsTypes.untracked(CmsTypes.double(selectionEff))

    setattr(process, modName, filterMod)
    setattr(process, triggerPath, Path(filterMod))

    return

            
    
def addTriggers(process, **triggers):
    """
    _addTriggers_

    Add the list of trigger names to the config.

    Triggers should be a map of trigger path name to selection
    efficiency. 

    """
    order = triggers.keys()
    order.sort()
    
    for trigName in order:
        makeTriggerEntry(process, trigName, triggers[trigName])
    return

def addOutputModule(process, fileName):
    """
    _addOutputModule_

    
    """
    outMod = OutputModule("EventStreamFileWriter")
    outMod.max_event_size = CmsTypes.untracked(CmsTypes.int32(7000000))
    outMod.max_queue_depth = CmsTypes.untracked(CmsTypes.int32(5))
    outMod.use_compression = CmsTypes.untracked(CmsTypes.bool(True))
    outMod.compression_level = CmsTypes.untracked(CmsTypes.int32(1))

    streamer = "%s.dat" % fileName
    indexFile = "%s.ind" % fileName
    outMod.fileName = CmsTypes.untracked(CmsTypes.string(streamer))
    outMod.indexFileName = CmsTypes.untracked(CmsTypes.string(indexFile))

    process.out = outMod
    process.o = EndPath(process.out)

    return


def setRunAndLumi(process, run, lumi):
    """
    _setRunAndLumi_

    Set the run and lumi number for the process provided
    via the EmptySource instance

    """
    process.source.firstRun = CmsTypes.untracked(CmsTypes.uint32(run))
    process.source.firstLuminosityBlock = CmsTypes.untracked(
        CmsTypes.uint32(lumi))
    return



def generateRun(runNumber, lumiCount, fileCount, eventCount, **triggers):
    """
    _generateRun_

    Generate a list of streamer creation config files for
    the run number provided.

    Will create a single run, with 0->lumiCount lumi sections
    and fileCount files per lumi section

    """
    result = []
    fileTotal = 0
    for lumi in range(0, lumiCount):
        for fileindx in range(0, fileCount):
            print " Creating config for run=%s lumi=%s file=%s" % (runNumber, lumi, fileTotal)

            process = makeProcess(eventCount)
            setRunAndLumi(process, runNumber, lumi)
            addTriggers(process, **triggers)
            addOutputModule(process, "streamer-out")
            result.append(process)
            fileTotal += 1
            

    return result


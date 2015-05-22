#!/usr/bin/env python
"""
_LumiProducer_

Util to generate a configuration PSet containing the lumi information to
be inserted into the repacking job.


"""
__version__ = "$Revision: 1.7 $"
__revision__ = \
    "$Id: LumiProducer.py,v 1.7 2008/06/26 16:02:16 evansde Exp $"


from FWCore.ParameterSet.Modules import EDProducer
from FWCore.ParameterSet.SequenceTypes import Path, Schedule
import FWCore.ParameterSet.Types as CmsTypes


def makeLumiProducer(process):
    """
    _makeLumiProducer_

    Create a new LumiProducer module in the process provided
    return a reference to it.

    Make sure that the lumi producer is added to the execution schedule
    
    """
    process.lumiProducer = EDProducer("LumiProducer")
    process.lumiPath = Path(process.lumiProducer)
    process.schedule = Schedule()
    process.schedule.append(process.lumiPath)
    endPath = getattr(process, "outputPath", None)
    if endPath != None:
        process.schedule.append(endPath)
    return process.lumiProducer


def makeLumiSection(lumiProducer, lumiData):
    """
    _makeLumiSection_

    Add a Lumi Section PSet to the lumi Producer module

    """
    psetName = "LS%s" % str(lumiData['lsnumber'])
    psetRef = CmsTypes.untracked( CmsTypes.PSet())
    setattr(lumiProducer, psetName, psetRef)

    psetRef.avginsdellumi = CmsTypes.untracked(
        CmsTypes.double(lumiData['avginslumi']))
    psetRef.avginsdellumierr = CmsTypes.untracked(
        CmsTypes.double(lumiData['avginslumierr']))
    psetRef.lumisecqual = CmsTypes.untracked(
        CmsTypes.int32(int(lumiData['lumisecqual'])))
    psetRef.deadfrac = CmsTypes.untracked(
        CmsTypes.double(lumiData['deadfrac']))
    psetRef.lsnumber = CmsTypes.untracked(
        CmsTypes.int32(lumiData['lsnumber']))

    psetRef.lumietsum = CmsTypes.untracked(CmsTypes.vdouble())
    psetRef.lumietsum = lumiData['det_et_sum']

    psetRef.lumietsumerr = CmsTypes.untracked(CmsTypes.vdouble())
    psetRef.lumietsumerr = lumiData['det_et_err']

    psetRef.lumietsumqual = CmsTypes.untracked(CmsTypes.vint32())
    psetRef.lumietsumqual = lumiData['det_et_qua']

    
    psetRef.lumiocc = CmsTypes.untracked(CmsTypes.vdouble())
    psetRef.lumiocc = lumiData['det_occ_sum']

    psetRef.lumioccerr = CmsTypes.untracked(CmsTypes.vdouble())
    psetRef.lumioccerr = lumiData['det_occ_err']

    psetRef.lumioccqual = CmsTypes.untracked(CmsTypes.vint32())
    psetRef.lumioccqual = lumiData['det_occ_qua']
    
    
    return


def insertEmptyLumiProducer(process):
    """
    _insertEmptyLumiProducer_

    Insert an empty producer to add default lumi information
    
    """
    allProducers = process.producers_()
    if not allProducers.has_key('lumiProducer'):
        makeLumiProducer(process)

    lumiProd = process.producers_()['lumiProducer']

    return

    
def insertLumiProducer(process, *lumiSections):
    """
    _insertLumiProducer_

    Given the process and the lumi info returned from the server,
    add the lumiProducer to the configuration and populate it
    with the lumi data

    """
    allProducers = process.producers_()
    if not allProducers.has_key('lumiProducer'):
        makeLumiProducer(process)

    lumiProd = process.producers_()['lumiProducer']
    
    for lumiSection in lumiSections:
        makeLumiSection(lumiProd, lumiSection)

        
    return
    




#!/usr/bin/env python
"""
_InsertLumi_

Util module to take the lumi information from a job, retrieve the
lumi info from the Lumi Server and insert it into a cfg process
object

***Runtime Only***
Uses CMSSW Python libs

"""

#  //
# // Assume there is a list of lumi servers to allow fallbacks
#//
LumiServers = [
    "http://cmsmon.cern.ch/CMSLUMI/servlet/LumiServlet"
    ]


from T0.LumiData.LumiServerLink import LumiServerLink
from T0.LumiData.LumiServerLink import LumiServerError
def insertLumiDefault(cmsswProcess):
    """
    _insertLumiDefault_

    Fallback to install an empty lumi producer into the process in the event that
    Lumi data cannot be retrieved


    """
    try:
        from T0.LumiData.LumiProducer import insertEmptyLumiProducer
    except ImportError, ex:
        msg = "Unable to import CMSSW Modules:\n%s" % ex
        raise RuntimeError, msg

    insertEmptyLumiProducer(cmsswProcess)
    return


def insertLumi(run, lumis, cmsswProcess):
    """
    _insertLumi_

    Given the list of run/lumi information in the repack job entity,
    retrieve the lumi information and insert it into the config process
    instance

    - *repackJobEntity* : Instance of T0.DataStructs.RepackJobEntity

    - *cmsswProcess* : Instance of FWCore.ParameterSet.Config.Process

    """
    try:
        from T0.LumiData.LumiProducer import insertLumiProducer
    except ImportError, ex:
        msg = "Unable to import CMSSW Modules:\n%s" % ex
        raise RuntimeError, msg

    for lumiSvr in LumiServers:
	print "lumiSvr =", `lumiSvr`
        lumiLink = LumiServerLink(lumiSvr)
        try:
            lumiInfo = lumiLink(run, *lumis)
            print "Aggregated Lumi Information:", lumiInfo
            insertLumiProducer(cmsswProcess, *lumiInfo.values())
            return
        except LumiServerError, le:
            print str(le)
            #import pdb
            #pdb.set_trace()
            continue  
        except Exception, ex:
            msg = "Exception calling lumi server:\n%s\n" % str(ex)
            print msg
            continue
    msg = "Unable to retrieve lumi information from lumi servers:\n"
    for lumiSrv in LumiServers:
        msg += "%s\n" %(lumiSrv)
    msg += "For Run: %s Lumis: %s\n" % (run, lumis)
    msg += "Installing default LumiProducer..."
    print msg

    insertLumiDefault(cmsswProcess)
    return




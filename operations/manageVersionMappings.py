#!/usr/bin/env python
"""
_manageVersionMappings_

A script to manage the repack version mappings in T0AST.
"""

__revision__ = "$Id: manageVersionMappings.py,v 1.1 2008/12/24 14:57:38 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import sys

from T0.State.Database.Writer import InsertRunConfig

from T0.GenericTier0 import Tier0DB
from ProdAgentCore.Configuration import loadProdAgentConfiguration

def printUsage():
    """
    _printUsage_

    Print the usage statement for this script.
    """
    print "To add or update a mapping:"
    print "  ./managerVersionMappings ONLINEVERSION OFFLINEVERSION"
    print ""
    print "Any other usage will print out the current mappings."
    print ""

    return

def printVersionMappings(dbConn):
    """
    _printVersionMappings_

    Print our the repacking version mappings that are currently registered
    in T0AST.
    """
    sqlQuery = """select DISTINCT rver_cmssw.name AS run_version,
                  rever_cmssw.name as repack_version from
                  repack_run_version_assoc repack_assoc join
                  cmssw_version rver_cmssw on repack_assoc.run_version =
                  rver_cmssw.id join cmssw_version rever_cmssw on
                  repack_assoc.repack_version = rever_cmssw.id"""

    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()

    print "Repacking version mappings in T0AST"
    print "  (reported version) -> (version used for repacking)"
    for result in results:
        print "  %s -> %s" % (result[0], result[1])

    return

def updateVersionMapping(dbConn, onlineVersion, offlineVersion):
    """
    _updateVersionMapping_

    Update or add a version mapping.  Delete any mappings that may exist for
    the online version and then add the new versions and mapping.
    """
    print "Mapping %s to %s" % (onlineVersion, offlineVersion)
    
    deleteQuery = """DELETE FROM repack_run_version_assoc WHERE run_version =
                    (SELECT id FROM cmssw_version WHERE name = :p_1)"""
    bindVars = {"p_1": onlineVersion}

    dbConn.execute(deleteQuery, bindVars)
    
    InsertRunConfig.insertCMSSWVersion(dbConn, onlineVersion)
    InsertRunConfig.insertCMSSWVersion(dbConn, offlineVersion)
    
    InsertRunConfig.insertRepackVersionMappings(dbConn,
                                                {onlineVersion: offlineVersion})
    dbConn.commit()
    return

if __name__ == "__main__":
    paConfig = loadProdAgentConfiguration()    
    t0astDBConfig = paConfig.getConfig("Tier0DB")

    t0astDBConn = Tier0DB.Tier0DB(t0astDBConfig)
    t0astDBConn.connect()

    if len(sys.argv) == 3:
        updateVersionMapping(t0astDBConn, sys.argv[1], sys.argv[2])
    else:
        printUsage()
        
    printVersionMappings(t0astDBConn)
    t0astDBConn.commit()
    t0astDBConn.close()
    sys.exit(0)

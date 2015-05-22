#!/usr/bin/env python2.4
"""
_cullRecoData_

Pull information from T0AST about reconstruction.
"""

__revision__ = "$Id: cullRecoData.py,v 1.1 2008/10/22 16:17:43 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import sys

from T0.GenericTier0 import Tier0DB
from ProdAgentCore.Configuration import loadProdAgentConfiguration

def queryT0AST(dbConn, firstRun, lastRun):
    """
    _queryT0AST_

    Query T0AST for information about reco jobs and files.
    """
    sqlQuery = """SELECT run_id, job_id, definition_time, completion_time,
                  retry_count, events FROM promptreco_job_def INNER JOIN
                  promptreco_job_repack_assoc USING (job_id) INNER JOIN
                  repacked USING (repacked_id) WHERE completion_time IS NOT
                  Null AND run_id >= :p_1"""

    if lastRun == -1:
        bindVars = {"p_1": firstRun}
    else:
        sqlQuery += " AND run_id <= :p_2"
        bindVars = {"p_1": firstRun, "p_2": lastRun}

    t0astDBConn.execute(sqlQuery, bindVars)
    results = t0astDBConn.fetchall()
    t0astDBConn.commit()    

    for result in results:
        print "%s,%s,%s,%s,%s,%s" % (resultRow[0], resultRow[1], resultRow[2],
                                     resultRow[3], resultRow[4], resultRow[5])

    return

def printUsage():
    """
    _printUsage_

    Print the usage statement to stdout.
    """
    print "Usage:"
    print "  ./cullRecoData.py --firstRun=RUNNUMBER [--lastRun=RUNNUMBER]"
    print ""
    print "Data is dumped to stdout in the form of comma delimited strings."
    print "The times are represented as the number of seconds since midnight"
    print "on January 1, 1970."
    print "The format is:"
    print "  RUN NUMBER,JOB ID,DEFINITION TIME,COMPLETION TIME, RETRY COUNT, EVENTS"

    return

if __name__ == "__main__":
    if len(sys.argv) != 2 and len(sys.argv) != 3:
        printUsage()
        sys.exit(0)

    firstRun = -1
    lastRun = -1

    if sys.argv[1].find("--firstRun=") == 0:
        (junk, firstRun) = sys.argv[1].split("=")
    elif sys.argv[1].find("--lastRun=") == 0:
        (junk, lastRun) = sys.argv[1].split("=")
    else:
        printUsage()
        sys.exit(0)

    if len(sys.argv) == 3:
        if sys.argv[2].find("--firstRun=") == 0:
            (junk, firstRun) = sys.argv[1].split("=")
        elif sys.argv[2].find("--lastRun=") == 0:
            (junk, lastRun) = sys.argv[1].split("=")
        else:
            printUsage()
            sys.exit(0)        

    if firstRun == -1:
        printUsage()
        sys.exit(0)
        
    paConfig = loadProdAgentConfiguration()
    t0astDBConfig = paConfig.getConfig("Tier0DB")
    
    t0astDBConn = Tier0DB.Tier0DB(t0astDBConfig)
    t0astDBConn.connect()

    queryT0AST(t0astDBConn, firstRun, lastRun)
    
    t0astDBConn.close()
    sys.exit(0)
                            

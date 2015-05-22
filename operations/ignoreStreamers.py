#!/usr/bin/env python2.4
"""
_ignoreStreamers_

Utility to mark bad streamers in T0AST so that the SegmentInjector
ignores them.
"""

__revision__ = "$Id: ignoreStreamers.py,v 1.3 2009/07/02 20:12:14 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

import sys

from T0.State.Database.Reader import ListStreamers
from T0.State.Database.Writer import InsertStreamer

from T0.GenericTier0 import Tier0DB

from ProdAgentCore.Configuration import loadProdAgentConfiguration

def ignoreStreamersForRun(dbConn, runNumber):
    """
    _ignoreStreamersForRun_

    Mark all the streamers for a specifc run as split so that the
    SegmentInjector ignores them.
    """
    unsplitStreamers = ListStreamers.listSplitableStreamersByRun(dbConn,
                                                                 runNumber)

    if len(unsplitStreamers) == 0:
        print "No unsplit streamers for run %s." % runNumber
        return

    streamersToIgnore = []
    print "Ignoring the following:"
    for unsplitStreamer in unsplitStreamers:
        print "  ID: %s, EVENTS: %s, LFN: %s" % (unsplitStreamer["STREAMER_ID"],
                                                 unsplitStreamer["EVENTS"],
                                                 unsplitStreamer["LFN"]) 
        streamersToIgnore.append(unsplitStreamer["STREAMER_ID"])

    InsertStreamer.updateSplitStreamers(dbConn, streamersToIgnore)
    return

def ignoreStreamer(dbConn, streamerID):
    """
    _ignoreStreamer_

    Mark a specific streamer as split so that the SegmentInjector ignores
    it.
    """
    unsplitStreamer = ListStreamers.listSplitableStreamerByID(dbConn,
                                                              streamerID)

    if unsplitStreamer == None:
        print "Don't know anything about streamer %s." % streamerID
        return

    print "Ignoring the following:"
    print "  ID: %s, EVENTS: %s, LFN: %s" % (unsplitStreamer["STREAMER_ID"],
                                             unsplitStreamer["EVENTS"],
                                             unsplitStreamer["LFN"]) 

    InsertStreamer.updateSplitStreamers(dbConn,
                                        [{"STREAMER_ID": unsplitStreamer["STREAMER_ID"]}])
    return

if __name__ == "__main__":
    paConfig = loadProdAgentConfiguration()    
    t0astDBConfig = paConfig.getConfig("Tier0DB")
    
    t0astDBConn = Tier0DB.Tier0DB(t0astDBConfig)
    t0astDBConn.connect()

    if len(sys.argv) != 3:
        print "Usage:"
        print "  ./ignoreStreamers.py RUN RUN_NUMBER"
        print "  ./ignoreStreamers.py STREAMER STREAMER_ID"
        print ""
        sys.exit(0)

    if sys.argv[1] == "RUN":
        ignoreStreamersForRun(t0astDBConn, sys.argv[2])
    elif sys.argv[1] == "STREAMER":
        ignoreStreamer(t0astDBConn, sys.argv[2])
    else:
        print "Unknown command: %s" % sys.argv[1]

    t0astDBConn.commit()
    t0astDBConn.close()
    sys.exit(0)

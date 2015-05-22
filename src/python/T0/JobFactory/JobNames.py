#!/usr/bin/env python
"""
_JobNames_

Single entry point to retrieve Unique Job IDs.

This should call out the the DB to get a new unique job ID.

Needs to be threadsafe.

"""

# temporary measure
from ProdCommon.MCPayloads.UUID import makeUUID



def newRepackJobID(run):
    """
    _newRepackJobID_

    Get a new repacker job ID from the DB that is associated
    to a given run

    """
    
    return "Repacker-Run%s-%s" %(run, makeUUID())



def newRepackMergeJobID(run):
    """
    _newRepackMergeJobID_

    Return a new repacker merge job ID from the DB that is associated
    to a given run

    """
    return "RepackerMerge-Run%s-%s" %(run, makeUUID())

def newConvJobID():
    """
    _newConvJobID_

    Return a new conversion job ID

    """
    return "Conversion-%s" % makeUUID()

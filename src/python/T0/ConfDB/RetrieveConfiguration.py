#!/usr/bin/env python
"""
_RetriveConfiguration_

Top level API to retrieve the configuration for a run from ConfDB.

Majority of access to ConfDB should be done through the RunConfigCache
utilities which will invoke these APIs as needed.

"""

import logging

from T0.ConfDB.ConfDBAPI import ConfDB

def getConfiguration(run, hltkey):
    """
    _getConfiguration_

    Query ConfDB with the HLT configuration name for the
    details of streams, primary datasets and trigger paths.

    Just return None in case of problems and let the caller deal with it
    """

    logging.info("Trying to retrieve configuration for run %d" % run)

    confDB = ConfDB()

    try:
        confData = confDB(hltkey)
    except Exception, ex:
        logging.error("Could not get RunConfig for run %d: %s\n" % (run,ex))
        return None

    return confData

#!/usr/bin/env python
"""
_Cache_

In memory cache for a set of RunConfig objects.
Maximum number of RunConfigs in memory configured via constructor.

In the event a RunConfig is not in memory, it is created from information in
T0AST.

If T0AST does not know about this Run, the information is looked up in
RunSummary and ConfDB and written into T0AST before loading it.
"""

__revision__ = "$Id: Cache.py,v 1.55 2009/05/29 17:38:20 hufnagel Exp $"
__version__ = "$Revision: 1.55 $"

_DEFAULT_CACHE_DEPTH = 100

import time
import operator
import logging

from T0.State.Database.Reader import ListRunConfig
from T0.State.Database.Reader import ListDatasets
from T0.State.Database.Writer import InsertDataset
from T0.State.Database.Writer import InsertRunConfig

import T0.ConfDB.RetrieveConfiguration as ConfDB
from T0.RunConfigCache.RunConfig import RunConfig
from T0.RunConfigCache.Tier0Config import retrieveDatasetConfig
from T0.RunConfigCache.Tier0Config import retrieveStreamConfig

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from WMCore.Configuration import loadConfigurationFile

class Cache:
    """
    _Cache_

    Access to Run configuration with in mem cache, T0AST lookup and
    ConfDB callouts
    """
    def __init__(self, cacheDepth = _DEFAULT_CACHE_DEPTH):
        self.depth = cacheDepth
        self.fileBackend = None
        self.t0astDBConn = None
        self.accessTimes = {}
        self.runConfigs = {}

        # The path to the Offline ConfDB file and the directory to cache
        # framework configs is stored in the RunConfig section of the ProdAgent
        # config file.
        paConfig = loadProdAgentConfiguration().getConfig("RunConfig")
        self.offlineConfDB = paConfig["OfflineConfDB"]
        self.configCache = paConfig["ConfigCacheDir"]
        
        logging.debug("RunConfigCache instantiated with depth=%s" % self.depth)

    def getRunConfig(self, runNumber):
        """
        _getRunConfig_

        Retrieve the configuration for a given run.
        """
        if self.runConfigs.has_key(runNumber):
            self.accessTimes[runNumber] = time.time()
            logging.debug("RunConfig %s accessed in memory at %s" %
                          (runNumber, self.accessTimes[runNumber]))
            return self.runConfigs[runNumber]

        #
        # Check if run exists in T0AST, if not pull
        # from RunSummary/ConfDB or Filebackend
        # and write it to T0AST
        #
        if not ListRunConfig.runExists(self.t0astDBConn, runNumber):
            try:
                self.insertRunConfig(runNumber)
            except Exception, ex:
                # Two things could have happened here:
                #   - Something bad happened.  We have a database connection
                #     problem, a config problem, etc.
                #   - We have a race between two components trying to insert the
                #     same run config.
                #
                # We'll assume that the problem is the latter.  Wait for a
                # second and then check to see if the RunConfig exists in T0AST.
                # If it does we're golden and we can move on.  If it doesn't
                # we have a real problem and we'll bail out.
                logging.error("Failed to insert run config: %s" % str(ex))
                self.t0astDBConn.rollback()
                time.sleep(1)

                if not ListRunConfig.runExists(self.t0astDBConn, runNumber):
                    return None

        newRunConfig = RunConfig(self.t0astDBConn, runNumber, self.configCache)
        self.addRunConfig(newRunConfig)
        return self.runConfigs[runNumber]

    def insertRunConfig(self, runNumber):
        """
        _insertRunConfig_

        Insert a run config into T0AST.  Pull down the offline configuration as
        well as the online configuration and insert everything into T0AST.
        """
        logging.debug("Run %s does not exist in T0AST" % runNumber)
        logging.debug("Pulling from RunSummary/ConfDB")

        # transfer system sets these, so they should always be present
        versionAndHLTKey = ListRunConfig.retrieveVersionAndHLTKey(self.t0astDBConn,
                                                                  runNumber)                  
        onlineVersion = versionAndHLTKey[0][0]
        hltkey = versionAndHLTKey[0][1]
        logging.debug( "onlineVersion: %s hltkey: %s" % ( onlineVersion, hltkey ) )

        tier0Config = loadConfigurationFile(self.offlineConfDB)

        repackVersion = tier0Config.Global.RepackVersionMappings.get(onlineVersion, None)
        expressVersion = tier0Config.Global.ExpressVersionMappings.get(onlineVersion, None)

        InsertRunConfig.updateRepackExpressVersion(self.t0astDBConn, runNumber,
                                                   repackVersion, expressVersion)

        configuration = ConfDB.getConfiguration(runNumber, hltkey)
                
        if configuration == None:
            raise RuntimeError, "Could not retrieve HLT config for run %s" % runNumber

        InsertRunConfig.insertRunConfig(self.t0astDBConn, runNumber,
                                        configuration[0], configuration[1],
                                        tier0Config.Global.AcquisitionEra)

        InsertRunConfig.insertT0Config(self.t0astDBConn, runNumber,
                                       tier0Config.Global.Version)

        for streamName in configuration[1].keys():
            streamConfig = retrieveStreamConfig(tier0Config, streamName)
            InsertRunConfig.insertStreamConfig(self.t0astDBConn, runNumber,
                                               streamName, streamConfig)

            for datasetName in configuration[1][streamName]:
                datasetConfig = retrieveDatasetConfig(tier0Config, datasetName)
                InsertRunConfig.insertDatasetConfig(self.t0astDBConn, runNumber,
                                                    datasetConfig)
                        
        self.t0astDBConn.commit()
        return

    def addRunConfig(self, runConfig):
        """
        _addRunConfig_

        Add a new run config, make an entry in the access times and bump the last config
        out if the cache is at capacity
        """
        self.runConfigs[runConfig.getRunNumber()] = runConfig
        self.accessTimes[runConfig.getRunNumber()] = time.time()
        logging.debug("Adding RunConfig %s @ %s" % \
                      (runConfig.getRunNumber(),
                       self.accessTimes[runConfig.getRunNumber()]))
        if len(self.runConfigs.keys()) > self.depth:
            self.dropOldestRunConfig()

    def dropOldestRunConfig(self):
        """
        _dropOldestRunConfig_

        Drop the oldest RunConfig from the cache.
        """
        accessTimeList = self.accessTimes.items()
        accessTimeList.sort(key = operator.itemgetter(1))

        if len(accessTimeList) == 0:
            return 

        oldRunNumber = accessTimeList[0][0]
        self.dropRunConfig(oldRunNumber)
        
    def dropRunConfig(self, runNumber):
        """
        _dropRunConfig_

        Drop the config for the run provided from the cache.
        """
        del self.runConfigs[runNumber]
        del self.accessTimes[runNumber]
        logging.debug("Run %s dropped" % runNumber)
        return

#!/usr/bin/env python
"""
_ConfDBAPI_

"""
__version__ = "$Revision: 1.7 $"
__revision__ = "$Id: ConfDBAPI.py,v 1.7 2009/03/12 14:32:28 hufnagel Exp $"


import logging

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from T0.GenericTier0 import Tier0DB


class ConfDB:
    """
    _ConfDB_

    API to access ConfDB information directly from the database

    """
    def __call__(self, configName):
        """
        _operator()_

        Retrieve the stream/dataset/path mapping for a given config name

        """
        # temporary shortcut for the Tier0 Relval
        if ( configName == 'Tier0Relval' ):
            streamsToDatasets = { 'A' : [ 'RelvalDataset1', 'RelvalDataset2' ] }
            datasetsToPaths = { 'RelvalDataset1' : [ 'HLT_Jet30' ],
                                'RelvalDataset2' : [ 'HLT_Jet110' ] }
            return { "Streams" : streamsToDatasets,
                     "Datasets": datasetsToPaths,
                     "Process" : 'HLT',
                     }

        paConfig = loadProdAgentConfiguration()
        confDBConfig = paConfig.getConfig("ConfDB")

        confDBConn = Tier0DB.Tier0DB(confDBConfig,
                                     manageGlobal = False)

        confDBConn.connect()

        sqlQuery = """SELECT a.streamLabel, c.datasetLabel, e.name, g.processName FROM Streams a
                      INNER JOIN ConfigurationStreamAssoc b
                      ON b.streamId = a.streamId
                      INNER JOIN PrimaryDatasets c
                      ON c.datasetId = b.datasetId
                      INNER JOIN PrimaryDatasetPathAssoc d
                      ON d.datasetId = c.datasetId
                      INNER JOIN Paths e
                      ON e.pathId = d.pathId
                      INNER JOIN ConfigurationPathAssoc f
                      ON f.pathId = e.pathId
                      INNER JOIN Configurations g
                      ON g.configId = f.configId AND g.configId = b.configId
                      WHERE g.configDescriptor = :p_1"""

        bindVars = {"p_1": configName}
        confDBConn.execute(sqlQuery, bindVars)
        results = confDBConn.fetchall()

        processName = None
        mapping = {}

        for result in results:

            stream = result[0]
            dataset = result[1]
            path = result[2]
            processName = result[3]
    
            if mapping.has_key(stream):
                if mapping[stream].has_key(dataset):
                    mapping[stream][dataset].append(path)
                else:
                    mapping[stream][dataset] = [path]
            else:
                mapping[stream] = { dataset : [path] }

        if len(mapping) == 0:
            errorMsg = "Couldn't extract stream-dataset-path mapping."
            raise Exception, errorMsg

        logging.info("Mapping extracted for config %s" % configName)

        return [ processName, mapping ]
        

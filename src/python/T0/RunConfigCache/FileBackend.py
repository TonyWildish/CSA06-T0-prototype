#!/usr/bin/env python
"""
_FileBackend_

File backend for RunConfig Cache tests

"""

import logging

from T0.RunConfigCache.RunConfigCacheError import RunConfigCacheError


class FileBackend(list):
    """
    _FileBackend_

    For development/non-DB use, a developer can provide a file
    containing a list of pickled RunConfig objects
    to act as a substitute for T0AST/ConfDB
    
    """
    def __init__(self, filename):
        list.__init__(self)
        self.filename = filename


    def getConfiguration(self, runNumber):
        """
        _getRunConfig_

        Get the RunConfig (not run dependent at the moment)

        Throws RunConfigCacheError if not found

        """
        globalDict = {}

        execfile(self.filename,globalDict)

        mapping = {}
        for stream, datasets in globalDict['streamToDatasets'].iteritems():
            mapping[stream] = {}
            for dataset in datasets:
                mapping[stream][dataset] = globalDict['datasetToTriggers'][dataset]

        return [ globalDict['cmssw'], globalDict['process'], mapping ]

        #msg = "Run %s not known to " % runNumber
        #msg += "RunConfigCache File Backend:\n"
        #msg += str(self.file)
        #raise RunConfigCacheError(msg)
    



#!/usr/bin/env python
"""
_StreamerIndex_

Details extracted from a streamer file index, containing the
details of the composition of the file in terms of events per trigger
path

Also reads/writes XML format consistent with the output of edmStreameIndex
tool.

"""

from T0.DataStructs.TriggerPath import TriggerPath

class StreamerIndex(dict):
    """
    _StreamerIndex_

    Streamer Index file container

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("Run", None)
        self.setdefault("Lumi", None)
        self.setdefault("TotalEvents", 0)
        self.triggerPaths = {}
        

    def addTriggerPath(self, pathname, eventCount = 0, errorCount = 0):
        """
        _addTriggerPath_

        Add a Trigger Path to this streamer, specifying the number
        of events and errors if supplied

        """
        newPath = TriggerPath()
        newPath['Name'] = pathname
        newPath['EventCount'] = eventCount
        newPath['ErrorCount'] = errorCount
        self.triggerPaths[pathname] = newPath
        return

    

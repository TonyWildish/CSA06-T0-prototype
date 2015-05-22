#!/usr/bin/env python
"""
_TriggerSegment_

Data Object representing a trigger segment and the information
related to it

"""



class TriggerSegment(dict):
    """
    _TriggerSegment_

    Dict based object for containing information about a Trigger
    Segment entity
    
    """
    def __init__(self, **args):
        dict.__init__(self)
        #self.setdefault("SegmentID", None)
        self.setdefault("DatasetID", None) # Internal DB ID
        self.setdefault("StreamerID", None)       # Internal DB ID
        self.setdefault("LumiID", None)           # Actual Lumi ID
        self.setdefault("RunID", None)            # Actual Run number
        self.setdefault("SegmentSize", None)      
        self.setdefault("Status", None)
        self.update(args)


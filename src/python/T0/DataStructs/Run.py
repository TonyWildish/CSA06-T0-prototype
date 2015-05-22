#!/usr/bin/env python
"""
_Run_

Data Object representing a run table and the information
related to it

"""



class Run(dict):
    """
    _Run_

    Dict based object for containing information about a Run table
    
    """
    def __init__(self, **args):
        dict.__init__(self)
        self.setdefault("RunID", None)           # Actual Run number
        self.setdefault("StartTime", None)       
        self.setdefault("EndTime", None)         
        self.setdefault("AppName", None)          
        self.setdefault("AppVersion", None)      
        self.setdefault("RunStatus", None)
        self.update(args)


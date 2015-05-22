#!/usr/bin/env python
"""
_LumiSection_

Data Object representing a Lumi_Section table and the information
related to it

"""



class LumiSection(dict):
    """
    _LumiSection_

    Dict based object for containing information about a lumi_section table
    
    """
    def __init__(self, **args):
        dict.__init__(self)
        self.setdefault("LUMI_ID", None)          # lumi section number
        self.setdefault("RUN_ID", None)           # Actual Run number
        self.setdefault("START_TIME", None)       
        
        # not yet in the data base
        self.setdefault("END_TIME", None)         
        self.setdefault("StartEventNumber", None)          
        self.setdefault("EndEventNumber", None)      
        self.update(args)

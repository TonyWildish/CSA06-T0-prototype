#!/usr/bin/env python
"""
_T0ASTFile_

A dictionary based object meant to represent a file in T0AST.
"""

__revision__ = "$Id: T0ASTFile.py,v 1.22 2009/07/07 14:09:16 sryu Exp $"
__version__ = "$Revision: 1.22 $"

from WMCore.WMBS.File import File as WMBSFile
from T0.Globals import alcaNewSelection
 
class T0ASTFile(dict):
    """
    _T0ASTFile_

    A dictionary based object meant to represent a file in T0AST.
    It contains the following keys:
      ID
      STREAM
      RUN_ID
      FILESIZE
      EVENTS
      CKSUM
      LFN
      DATASET_ID
      DATA_TIER
      TYPE
      STATUS
      EXPORT_STATUS
      STREAM
      STREAM_ID
      LUMI_ID
      LUMI_LIST
      PARENT_LIST
      PSET_HASH
      BRANCH_HASH
      LOCATIONS
    """
    def __init__(self, wmbsFile=None, **args):
        """
        ___init___

        Initialize all attributes.
        """
        dict.__init__(self)
        
        self.runLumi = None
        # place holder for list of parent(repacked) files names
        # only used for reco type, for repacked this should be empty
        self.parentList = None
        self.parentIDList = None
        # this is the dataset_path_ID in data table not primary dataset id
        self.datasetPathID = None
        # place holder for PSet hash, Gets from Job Report
        self.setdefault("RUN_ID", None)
        self.setdefault("STREAM", None)
        self.setdefault("DATASET_ID", None)
        self.setdefault("PRIMARY_DATASET", None)
        self.setdefault("PROCESSED_DATASET", None)
        self.setdefault("DATA_TIER", None)        
        self.setdefault("BLOCK_ID", None)
        self.setdefault("STREAM", None)
        self.setdefault("STREAM_ID", None)
    # currently using WM Run structure here but need to use lumi structure 
        # place holder for list of lumi data struct
        #self.setdefault("LUMI_LIST", [])
            
        self.setdefault("PSET_HASH", None)
        self.setdefault("BRANCH_HASH", None)
            
        self.setdefault("LOCATIONS", None)
        self.wmbsFile = wmbsFile
        
        if wmbsFile == None:
            self.setdefault("ID", None)
            self.setdefault("FILESIZE", None)
            self.setdefault("EVENTS", None)
            self.setdefault("FIRST_EVENT", 0)
            self.setdefault("LAST_EVENT", 0)
            self.setdefault("CKSUM", None)        
            self.setdefault("LFN", None)
            # this is primary dataset ID
            self.update(args)
        else:        
            self["ID"] = wmbsFile["id"]
            self["FILESIZE"] = wmbsFile["size"]
            self["CKSUM"] = wmbsFile["cksum"]
            self["EVENTS"] = wmbsFile["events"]
            self["FIRST_EVENT"] = wmbsFile["first_event"]
            self["LAST_EVENT"] = wmbsFile["last_event"]
            self["LFN"] = wmbsFile["lfn"]
        
        

    def convertToWMBSFile(self):
        """
        __getWMBSFile__
        
        return WMBSFile instance converted from T0AST file.
        
        """
        if self.wmbsFile == None:
            # hack: make sure first_event/last_event is None
            # if they are add 0 event
            if self["FIRST_EVENT"] == None:
                self["FIRST_EVENT"] = 0
            if self["LAST_EVENT"] == None:
                self["LAST_EVENT"] = 0
            self.wmbsFile = WMBSFile(lfn=self["LFN"], size=self["FILESIZE"], 
                                    events=self["EVENTS"], cksum=self["CKSUM"],
                                    first_event = self["FIRST_EVENT"],
                                    last_event = self["LAST_EVENT"],
                                    locations=self["LOCATIONS"])
            self.wmbsFile.create()
            self["ID"] = self.wmbsFile["id"]
        
        return self.wmbsFile
    
    def getParentList(self, dataType='lfn', stream="Normal"):
        """
        _getParentList_
        """
        if self["DATA_TIER"] == "RAW":
            return []
        
        #TODO: place holder for correct express stream handling
        if stream == "Express":
            # TODO get using the lumi comparason
            # For now return empty list for parent 
            return []
        self.convertToWMBSFile()
        
        if dataType == "id":
            parents = self.parentIDList
        elif dataType == "lfn":
            parents = self.parentList
        else:
            raise Exception, "Unknown Type"
        
        if parents == None:
            # this will retrun right parents for both RECO -> RAW and 
            # RAW -> [] files except the big files merged directly
            if self["DATA_TIER"] == "ALCARECO" \
                and alcaNewSelection():
                parents = self.wmbsFile.getAncestors(
                                                    level=3, type=dataType)
            else:
                parents = self.wmbsFile.getAncestors(level=2, type=dataType)
            
            #update the private variable to prevent expensive call over and over again
            if dataType == "id":
                self.parentIDList = parents
            elif dataType == "lfn":
                self.parentList = parents
        return parents
    
    def getLumiList(self):
        """
        _getLumiList_
        """
        self.convertToWMBSFile()
        if self.runLumi == None:
            if len(self.wmbsFile["runs"]) == 0:
                self.wmbsFile.loadData()
            self.runLumi = list(self.wmbsFile["runs"])[0]
            self["RUN_ID"] = self.runLumi.run
        return self.runLumi
    
    def getRunID(self):
        """
        _getRunID_
        """
        self.convertToWMBSFile()
        if self["RUN_ID"] == None:
            if len(self.wmbsFile["runs"]) == 0:
                self.wmbsFile.loadData()
            self.runLumi = list(self.wmbsFile["runs"])[0]
            self["RUN_ID"] = self.runLumi.run
        return self["RUN_ID"]
    
    def getDatasetPathID(self):
        """
        _getDatasetPathID_
        
        To do: this need database connection to get the value.
        need to be modified like wmbs wrapper class
        """
        return self.datasetPathID
        

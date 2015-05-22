#!/usr/bin/env python
"""
_Block_

A dictionary based object meant to represent a row in the active_block/block table.
"""

__revision__ = "$Id: Block.py,v 1.21 2009/04/08 19:00:54 sryu Exp $"
__version__ = "$Revision: 1.21 $"

import logging
from T0.RunConfigCache.CacheManager import getRunConfigCache
from ProdCommon.MCPayloads.UUID import makeUUID

class Block(dict):
    """
    _Block_

    A dictionary based object meant to represent a row in the reconstructed table.
    It contains the following keys:
      BLOCK_ID
      RUN_ID
      DATASET_ID : primary dataset id
      DATASET_PATH_ID : dataset path id
      DATA_TIER : RAW, RECO, ALCARECO, AOD
      BLOCKSIZE
      FILECOUNT
      STATUS

    """
    def __init__(self, t0astFile=None, **args):
        """
        ___init___

        Initialize all attributes.
        If T0ASTFile is passed as parameter, create the block instance 
        using T0ASTFile by extracting necessary information 
        from given T0ASTFile and assign to a Block instance
        """
        dict.__init__(self)
        
        self.setdefault("BLOCK_ID", None)
        self.setdefault("STATUS", "Active")
        self.setdefault("MIGRATE_STATUS", "NotMigrated")    
        
        if t0astFile != None:
            # full path used in dbs
            self.setdefault("BLOCK_NAME", "/%s/%s/%s#%s" % 
                            (t0astFile["PRIMARY_DATASET"], 
                             t0astFile["PROCESSED_DATASET"],
                             t0astFile["DATA_TIER"],
                             makeUUID())
                            )
            self.setdefault("RUN_ID", t0astFile.getRunID())
            self.setdefault("DATASET_ID", t0astFile["DATASET_ID"])
            self.setdefault("DATASET_PATH_ID", t0astFile.getDatasetPathID())
            self.setdefault("DATA_TIER", t0astFile["DATA_TIER"])
            self.setdefault("BLOCKSIZE", t0astFile["FILESIZE"])
            self.setdefault("FILECOUNT", 1)
        else:
    
            self.setdefault("BLOCK_NAME", None)
            self.setdefault("RUN_ID", None)
            self.setdefault("DATASET_ID", None)
            self.setdefault("DATASET_PATH_ID", None)
            self.setdefault("DATA_TIER", None)
            self.setdefault("BLOCKSIZE", 0) # byte
            self.setdefault("FILECOUNT", 0)
        
        self.update(args)
    
#    @staticmethod        
#    def mapFileTypeToDataTier(type):
#        if type == "Repacked":
#            return "RAW"
#        elif type == "Reconstructed":
#            return "RECO"
#        else:
#            return None
#    
    def getRunConfigForBlock(self):
        """
        _getRunConfForBlock_
        
        This is used to hide the runConfigCache dependency from other module
        returns runConf related to a given block
        """
        runConfCache = getRunConfigCache()
        return runConfCache.getRunConfig(self["RUN_ID"])
        
        
    def getDatasetPath(self):
        """
        _getDatasetPath_
        disassembles the primary dataset path for DBS from block name
        (to reduce the additional database access when load the block from db)
        """
        if self["BLOCK_NAME"] != None:
            return self["BLOCK_NAME"].split("#")[0]
        else:
            return None 
            
    def getPrimaryDatasetName(self):
        """
        _getPrimaryDatasetName_
        disassembles the primary dataset name from block name
        (to reduce the additional database access when load the block from db)
        """
        if self["BLOCK_NAME"] != None:
            return self["BLOCK_NAME"].split("/")[1]
        else:
            return None 
        
    def getProcessedDatasetName(self):
        """
        _getProcessedDatasetName_
        disassembles the processed dataset name from block name
        (to reduce the additional database access when load the block from db)
        """
        
        if self["BLOCK_NAME"] != None:
            return self["BLOCK_NAME"].split('/')[2]
        else:
            return None
    
    def getDataTier(self):
        """
        _getDataTier_
        disassembles the processed dataset name from block name
        (to reduce the additional database access when load the block from db)
        """
        if self["DATA_TIER"] != None:
            return self["DATA_TIER"]
        if self["BLOCK_NAME"] != None:
            return self["BLOCK_NAME"].split('/')[3].split('#')[0]
        else:
            return None
        
    def getParentDatasetPath(self):
        """
        _getParentDatasetPath_
        
        """
        if self.getDataTier() == "RAW":
            return None
        elif self.getDataTier() == "RECO" or self.getDataTier() == "ALCARECO" \
             or self.getDataTier() == "AOD":
            # TODO get parent block from T0AST, extract datasetpath
            return None
        
#!/usr/bin/env python
"""
_InsertBlock_

Utilities to manipulate block, active block, block_run_assoc in T0AST.
"""

__revision__ = "$Id: InsertBlock.py,v 1.26 2009/06/18 16:29:46 sryu Exp $"
__version__ = "$Revision: 1.26 $"

import time
from T0.DataStructs.Block import Block

#state information might be used
def insertBlock(dbConn, t0astFile):
    """
    _insertBlock_

    Insert an entry in the block table,
    Insert an entry inT active_block table,
    Insert an entry in block_run_assoc table  
    
    return Block object with block ID, block size and file count information
    
    long with this method, updating export status for repacked and reconstructed
    should be in one transition
    """
    
    dbConn.execute("SELECT block_SEQ.nextval from dual")
    blockID = dbConn.fetchall()

    while type(blockID) == tuple or type(blockID) == list:
        blockID = blockID[0]
        
    #quick dirty fix. -- need to investigate
    #blockID = blockID[0]
    
    blockInfo = Block(t0astFile)    
    blockInfo["BLOCK_ID"] = blockID

    blockInfo["STATUS"] = "Active"
    
    sqlQuery = """ INSERT INTO BLOCK (id, dataset_path_id, name, block_size, file_count, status)
                   VALUES (:p_1, :p_2, :p_3, :p_4, :p_5, 
                           (SELECT id FROM block_status WHERE status = :p_6))"""
                                           
    params = {'p_1':blockInfo["BLOCK_ID"],
              'p_2':blockInfo["DATASET_PATH_ID"], 
              'p_3':blockInfo["BLOCK_NAME"], 
              'p_4':blockInfo["BLOCKSIZE"], 
              'p_5':blockInfo["FILECOUNT"], 
              'p_6':blockInfo["STATUS"]}               
    
    dbConn.execute(sqlQuery, params)
    
    # insert into active_block table
    params = {'p_1':blockInfo["BLOCK_ID"],
              'p_2':blockInfo["DATASET_PATH_ID"], 
              'p_3':blockInfo["RUN_ID"]}
    
    sqlQuery = """ INSERT INTO ACTIVE_BLOCK (block_id, dataset_path_id, run_id)
                               VALUES (:p_1, :p_2, :p_3)"""
               
    dbConn.execute(sqlQuery, params)
    
    # insert into block_run_assoc table
    params = {'p_1':blockInfo["BLOCK_ID"], 
              'p_2':blockInfo["RUN_ID"]}               
    
    sqlQuery = """ INSERT INTO BLOCK_RUN_ASSOC (BLOCK_ID, RUN_ID)
                               VALUES (:p_1, :p_2)"""
    
    dbConn.execute(sqlQuery, params)
        
    ###
    ## need to add "update repacked and reconstructed 
    ## for block id and state
    ##
    ## better in InsertRepacked or, here
    ## 
    ## 'BLOCK_ID' and 'STATUS' information was added by operation
    ###  
    return blockInfo


def insertBlockParentage(dbConn, repackFileIDs, outputBlockID):
    """
    _insertBlockParentage_
    
    insert into block_parentage table. recoFileID is the Reconstructed type 
    T0ASTFile ID which has a BlockID as outputBlockID
    The query will find corresponding block ID from the repacked table through
    repacked_reco_parentage.
    If the (input_block_id, output_block_id) exist, it won't insert to 
    block_parentage table
    
    """
    
    sqlQuery = """ INSERT INTO block_parentage (input_id, output_id)
                       ((SELECT block_id, :p_1 FROM wmbs_file_block_assoc 
                            WHERE file_id = :p_2) 
                        MINUS (SELECT input_id, output_id from block_parentage)
                       )
               """
    bindVars = []
    for fileID in repackFileIDs:           
        bindVars.append({'p_1':outputBlockID, 'p_2':fileID})
    dbConn.execute(sqlQuery, bindVars)
    return
 
def closeBlockByBlockID(dbConn, blockInfoList):
    """
    _closeBlockByBlockID_
    
    Remove the block from active block table when it is closed.
    (It should be one transaction with changing status in block table)
    """
    if type(blockInfoList) != list:
        blockInfoList = [blockInfoList]
        
    bindVars = []
    sqlQuery = "DELETE FROM active_block WHERE block_id = :p_1"    
        
    for blockInfo in blockInfoList:
        bindVars.append({"p_1": blockInfo["BLOCK_ID"]})
    
    dbConn.execute(sqlQuery, bindVars)
    
    updateBlockStatusByID(dbConn, blockInfoList, "Closed")
    
    for blockInfo in blockInfoList:
        #update blockInfo to synchronize with database
        blockInfo["STATUS"] = "Closed"
        
def updateBlockStatusByID(dbConn, blockInfoList, status):
    """
    _updateBlockStatusByID_
    
    Update the given blocks' status with given status.
    """
    if type(blockInfoList) != list:
        blockInfoList = [blockInfoList]
    
    sqlQuery = """UPDATE block SET status = (SELECT id FROM block_status
                  WHERE status = :p_1) WHERE id = :p_2"""
                  
    bindVars = []
    for blockInfo in blockInfoList:
        bindVars.append({"p_1": status, "p_2": blockInfo["BLOCK_ID"]})
        
    dbConn.execute(sqlQuery, bindVars)
    return

def updateBlockMigrateStatusByID(dbConn, blockInfoList, status):
    """
    _updateBlockStatusByID_
    
    Update the given blocks' migrate status with given status.
    """
    if type(blockInfoList) != list:
        blockInfoList = [blockInfoList]
    
    sqlQuery = """UPDATE block SET migrate_status = 
                     (SELECT id FROM block_migrate_status
                      WHERE status = :p_1) WHERE id = :p_2"""
                  
    bindVars = []
    for blockInfo in blockInfoList:
        bindVars.append({"p_1": status, "p_2": blockInfo["BLOCK_ID"]})
        
    dbConn.execute(sqlQuery, bindVars)
    return

def updateBlockDeleteStatus(dbConn, blockInfoList, status):
    """
    _updateBlockStatus_
    
    Update the given blocks' delete status with given status.
    """
    if type(blockInfoList) != list:
        blockInfoList = [blockInfoList]
    
    sqlQuery = """UPDATE block SET delete_status = 
                     (SELECT id FROM block_delete_status
                      WHERE status = :p_1) WHERE id = :p_2"""
                  
    bindVars = []
    for blockInfo in blockInfoList:
        bindVars.append({"p_1": status, "p_2": blockInfo["BLOCK_ID"]})
        
    dbConn.execute(sqlQuery, bindVars)
    return

def updateBlockSizeAndFileCount(dbConn, blockInfo):
    """
    _updateBlockSizeAndFileCount_
    """
    sqlQuery = " UPDATE block SET block_size = :p_1, file_count = :p_2 WHERE id = :p_3 "
                  
    bindVars = {"p_1": blockInfo["BLOCKSIZE"], "p_2": blockInfo["FILECOUNT"],
                "p_3": blockInfo["BLOCK_ID"]}
        
    dbConn.execute(sqlQuery, bindVars)
    return


def insertFileBlockAssoc(dbConn, fileID, blockID):
    """
    _insertFileBlockAssc_
    """
    sqlQuery = """ INSERT INTO wmbs_file_block_assoc (file_id, block_id) 
                   VALUES (:file_id, :block_id) """
    bindVars = {"file_id": fileID, "block_id": blockID}
    dbConn.execute(sqlQuery, bindVars)
    return
    
def updateExportStartTime(dbConn, blockID):
    """
    _updateExportStartTime_
    
    update export start time: it should be only updated one time when the 
    first file start to be exported
    """
    
    sqlQuery = """ UPDATE block SET export_start_time = :p_2
                   WHERE id = :p_1 and export_start_time IS NULL 
                   """
                   
    param = {"p_1": blockID, "p_2": int(time.time())}
    dbConn.execute(sqlQuery, param)
    return

def updateExportEndTime(dbConn, blockID):
    """
    _updateExportEndTime_
    
    update export start time: it should be only updated one time.
    if it is called multiple time for the same block, it will update
    the time. So multiple calling this function should be check in application
    level 
    """
    
    sqlQuery = """ UPDATE block SET export_end_time = :p_2
                   WHERE id = :p_1"""
    
    param = {"p_1": blockID, "p_2": int(time.time())}
    dbConn.execute(sqlQuery, param)    
    return

#!/usr/bin/env python
"""
_ListBlock_

Read operations related to the block and active_block tables in T0AST. 
"""

from T0.DataStructs.Block import Block 

def listActiveBlock(dbConn, t0astFile):    
    """
    _listActiveBlock_
    
    Retrieve block related information from active_block and
    repacked/reconstructed table.  Returns block object with updated
    parameters from t0astFile.
    """
    block = Block(t0astFile)
            
    sqlQuery = """ SELECT b.id, b.name, ab.dataset_path_id, b.block_size, b.file_count
                   FROM active_block ab INNER JOIN block b ON (b.id = ab.block_id)
                   WHERE ab.run_id = :p_1 AND ab.dataset_path_id = :p_2  
               """
             
    bindVars = {'p_1':block["RUN_ID"], 'p_2':block["DATASET_PATH_ID"]}
    
    dbConn.execute(sqlQuery, bindVars)
    
    # this should only return one or None
    blockRows = dbConn.fetchall()
        
    if len(blockRows) != 1:
        return None
    else:
        # update block information
        blockRow = blockRows[0]
        block["BLOCK_ID"] = blockRow[0]
        block["BLOCK_NAME"] = blockRow[1]
        block["DATASET_PATH_ID"] = blockRow[2]
        block["BLOCKSIZE"] = blockRow[3] 
        block["FILECOUNT"] = blockRow[4]
        
        return block     
    
    
def listActiveBlockByRun(dbConn, runID):
    
    """
    _listActiveBlockByRun_
    
    retrieve block related information from active_block 
    and repacked/reconstructed table
    returns block object with updated parameters from t0astFile
    
    """    
    sqlQuery = """ SELECT bl.id, bl.name, ab.dataset_path_id, bl.block_size, bl.file_count, 
                          dp.primary_dataset 
                   FROM active_block ab 
                    INNER JOIN block bl ON (bl.id = ab.block_id) 
                    INNER JOIN dataset_path dp ON (dp.id =ab.dataset_path_id)
                   WHERE ab.run_id = :p_1
               """             
    bindVars = {'p_1':runID}
    
    dbConn.execute(sqlQuery, bindVars)
    # this should only return one or None
    blockRows = dbConn.fetchall()
    listBlocks = []
    for blockRow in blockRows:
        block = Block()
        block["BLOCK_ID"] = blockRow[0]
        block["BLOCK_NAME"] = blockRow[1]
        block["DATASET_PATH_ID"] = blockRow[2]
        block["BLOCKSIZE"] = blockRow[3]
        block["FILECOUNT"] = blockRow[4] 
        block["DATASET_ID"] = blockRow[5]
        block["RUN_ID"] = runID
        listBlocks.append(block)
    
    return listBlocks

def listBlocksByStatus(dbConn, statusList, migrateStatus = None, deleteStatus = None):
    """
    _listBlocksByStatus_
    list of blocks by status
    
    """
    
    sqlQuery = """SELECT block.id, dataset_path_id, name, run_id, 
                          dataset_path.primary_dataset FROM block
                   INNER JOIN block_run_assoc ON (block.id = block_id)
                   INNER JOIN dataset_path
                      ON (block.dataset_path_id = dataset_path.id)
                   WHERE status = (SELECT id FROM block_status WHERE status = :p_1)"""
    
    if type(statusList) == str:
        statusList = [statusList]
 
    if migrateStatus != None:
        sqlQuery += """ AND migrate_status = (SELECT id FROM block_migrate_status 
                        WHERE status = :mStatus) """
    
    if deleteStatus != None:
        sqlQuery += """ AND delete_status = (SELECT id FROM block_delete_status 
                        WHERE status = :dStatus) """
    
        
    bindVars = []
    for status in statusList:    
        params = {'p_1': status}               
        if migrateStatus != None:
            params['mStatus'] = migrateStatus
        if deleteStatus != None:
            params['dStatus'] = deleteStatus
            
        bindVars.append(params)
    # this is hack to order by tier: TODO order by tier
    # sqlQuery += """ ORDER BY block.id"""  
    dbConn.execute(sqlQuery, bindVars)
    
    blockRows = dbConn.fetchall()
    blockList = []
    
    for blockRow in blockRows:
        block = Block()
        block["BLOCK_ID"] = blockRow[0]
        block["DATASET_PATH_ID"] = blockRow[1]
        block["BLOCK_NAME"] = blockRow[2]
        block["RUN_ID"] = blockRow[3]
        block["DATASET_ID"] = blockRow[4]
        blockList.append(block)
    
    return blockList           

def getBlockInfoByID(dbConn, blockID):
    """
    _getBlockInfoByID_
    get information from the block table by block_id
    
    """
    sqlQuery = """ SELECT block.id, dataset_path_id, name, block_status.status,
                          bms.status 
                   FROM block
                   INNER JOIN block_status on block.status = block_status.id
                   INNER JOIN block_migrate_status bms on block.status = bms.id
                   WHERE block.id=:p_1 """
    param = {'p_1': blockID}
    dbConn.execute(sqlQuery, param)
    
    blockRows = dbConn.fetchall()
    blockRow = blockRows[0]
    block = Block()
    block["BLOCK_ID"] = blockRow[0]
    block["DATASET_PATH_ID"] = blockRow[1]
    block["BLOCK_NAME"] = blockRow[2]
    block["STATUS"] = blockRow[3]
    block["MIGRATE_STATUS"] = blockRow[4]
    
    return block

def getBlockIDByFileID(dbConn, fileID):
    """
    _getBlockIDByFileID_
    get block_id from wmbs_file_block_assoc
    
    """
    sqlQuery = "SELECT block_id FROM wmbs_file_block_assoc WHERE file_id=:p_1"
    param = {'p_1': fileID}
    dbConn.execute(sqlQuery, param)
    result = dbConn.fetchall()
    
    if len(result) == 0:
            return None
    else:
        return result[0][0]

def listBlockInfoByStatus(dbConn, transferStatus, migrationStatus):
    """
    _listBlockInfoByStatus_

    Retrieve a list of blocks with a given transfer status and migration status.
    The result will be a list of dictionaries each with the following keys:
    BLOCK_ID, DATASET_PATH_ID, RUN_ID, STORAGE_NODE, PRIMARY_ID,
    PROCESSED_ID and TIER_ID. 
    """
    sqlQuery = """SELECT block.id, block.name, block.dataset_path_id,
                         block_run_assoc.run_id,
                         storage_node.name, dataset_path.primary_dataset,
                         dataset_path.processed_dataset, dataset_path.data_tier
                  FROM block
                    INNER JOIN block_run_assoc
                      ON block.id = block_run_assoc.block_id
                    RIGHT OUTER JOIN phedex_subscription_made psm
                      ON block.dataset_path_id = psm.dataset_path_id
                    INNER JOIN storage_node
                      ON psm.node_id = storage_node.id
                    INNER JOIN dataset_path
                      ON block.dataset_path_id = dataset_path.id
                   WHERE block.status =
                    (SELECT id FROM block_status WHERE status = :p_1) AND
                   block.migrate_status =
                    (SELECT id FROM block_migrate_status WHERE status = :p_2)
                   """
    bindVars = {"p_1": transferStatus, "p_2": migrationStatus}

    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    blocks = []
    for result in results:
        blockInfo = Block()
        blockInfo["BLOCK_ID"] = result[0]
        blockInfo["BLOCK_NAME"] = result[1] 
        blockInfo["DATASET_PATH_ID"] = result[2]
        blockInfo["RUN_ID"] = result[3]
        blockInfo["STORAGE_NODE"] = result[4]
        blockInfo["PRIMARY_ID"] = result[5]
        blockInfo["PROCESSED_ID"] = result[6]
        blockInfo["TIER_ID"] = result[7]
        blocks.append(blockInfo)

    return blocks

def countBlocksByStatus(dbConn, runNumber, status):
    """
    _countBlocksByStatus_

    Count the number of block for a particular run with a particular status.
    """
    sqlQuery = """SELECT count(*) FROM block
                    INNER JOIN block_status ON block.status = block_status.id
                    INNER JOIN block_run_assoc on block.id = block_run_assoc.block_id
                  WHERE block_status.status = :p_1 AND
                        block_run_assoc.run_id = :p_2"""
    bindVars = {"p_1": status, "p_2": runNumber}

    dbConn.execute(sqlQuery, bindVars)
    return int(dbConn.fetchall()[0][0])

def listBlockByRun(dbConn, runNumber):
    """
    Retrieve blocks in a given run.
    TODO: Depends on which run status it will get called,
    it might return the different list of blocks. This is generally
    needed to be called when run processing is completed, if full list is
    needed.
    """
    sqlQuery = """ SELECT block.id, dataset_path_id, name, block_status.status,
                          primary_dataset
                   FROM block
                     INNER JOIN block_status on (block.status = block_status.id)
                     INNER JOIN block_run_assoc on (block_id = block.id)
                     INNER JOIN dataset_path dp ON (dp.id = dataset_path_id)
                   WHERE run_id=:p_1 """
    
    param = {'p_1': runNumber}
    dbConn.execute(sqlQuery, param)
    
    blockList = []
    blockRows = dbConn.fetchall()
    for blockRow in blockRows:
        block = Block()
        block["BLOCK_ID"] = blockRow[0]
        block["DATASET_PATH_ID"] = blockRow[1]
        block["BLOCK_NAME"] = blockRow[2]
        block["STATUS"] = blockRow[3]
        block["DATASET_ID"] = blockRow[4]
        block["RUN_ID"] = runNumber
        blockList.append(block)
        
    return blockList

def listCustodialNodeForBlock(dbConn, blockID):
    """
    _listCustodialNodeForBlock_

    Retrieve the name of the custoidal site for a given block.
    """
    sqlQuery = """SELECT storage_node.name FROM storage_node
                    INNER JOIN phedex_subscription_made ON storage_node.id = phedex_subscription_made.node_id
                    INNER JOIN block ON phedex_subscription_made.dataset_path_id = block.dataset_path_id
                    INNER JOIN block_run_assoc ON block.id = block_run_assoc.block_id
                    INNER JOIN dataset_path ON block.dataset_path_id = dataset_path.id
                    INNER JOIN primary_dataset ON dataset_path.primary_dataset = primary_dataset.id
                    INNER JOIN phedex_subscription ON phedex_subscription.run_id = block_run_assoc.run_id AND
                      phedex_subscription.primary_dataset_id = primary_dataset.id
                  WHERE block.id = :blockid AND phedex_subscription.custodial_flag = 1"""
    bindVars = {"blockid": blockID}

    dbConn.execute(sqlQuery, bindVars)
    custodialNodes = []
    for result in dbConn.fetchall():
        custodialNodes.append(result[0])
        
    return custodialNodes

def isParentBlockMigrated(dbConn, blockID):
    """
    this will return True if the block doesn't have a parent.
    """
    sqlQuery = """SELECT count(*) FROM block 
                    INNER JOIN block_parentage ON block.id = input_id
                  WHERE output_id = :blockID 
                        AND migrate_status != 
                            (SELECT id FROM block_migrate_status 
                             WHERE status = 'Migrated')"""
    
    bindVars = {"blockID": blockID}

    dbConn.execute(sqlQuery, bindVars)
    if (dbConn.fetchall()[0][0] == 0):
        return True
    else:
        return False
    
def _isParentBlockInStatus(dbConn, childBlockID, statusList):
    """
    _isParentBlockInStatus_
    
    check the all parent blocks in given statusList.
    return True if and only if all the parent blocks are in given states, 
    other wise False
    """
    sqlQuery = """SELECT count(*) FROM block
                   INNER JOIN block_status on block.status = block_status.id 
                   INNER JOIN block_parentage ON block.id = output_id
                  WHERE input_id = :childBlockID AND 
                        block_status.status NOT IN ("""
    
    bindVars = {"childBlockID": childBlockID}
    i = 0
    for status in statusList:
        i += 1
        param = ":p_%s" % i 
        sqlQuery += "%s, " % param
        bindVars[param] = status
    sqlQuery = sqlQuery.strip(', ') + ')'
    
    dbConn.execute(sqlQuery, bindVars)
    if (dbConn.fetchall()[0][0] == 0):
        return True
    else:
        return False

def isParentBlockExported(dbConn, childBlockID):
    """
    _isParentBlockExported_
    
    check the all parent blocks are exported.
    """
    return _isParentBlockInStatus(dbConn, childBlockID, ['Exported', 'Skimmed'])

def isParentBlockClosed(dbConn, childBlockID):
    """
    _isParentBlockClosed_
    
    check the all parent blocks are closed.
    """
    return _isParentBlockInStatus(dbConn, childBlockID, 
                                  ['Closed', 'InFlight', 'Exported', 'Skimmed'])

def listParentBlocks(dbConn, childBlockID):
    """
    _listParentBlocks_
    
    list all the parent blocks in block format by given childBlockID
    """
    sqlQuery = """SELECT block.id, dataset_path_id, name, run_id 
                  FROM block
                   INNER JOIN block_run_assoc ON (block.id = block_id)
                   INNER JOIN block_parentage ON (block.id = output_id) 
                  WHERE input_id = :childBlockID"""
    
    bindVars = {"childBlockID": childBlockID}
    
    dbConn.execute(sqlQuery, bindVars)
    
    blockRows = dbConn.fetchall()
    blockList = []
    
    for blockRow in blockRows:
        block = Block()
        block["BLOCK_ID"] = blockRow[0]
        block["DATASET_PATH_ID"] = blockRow[1]
        block["BLOCK_NAME"] = blockRow[2]
        block["RUN_ID"] = blockRow[3]
        blockList.append(block)
    
    return blockList

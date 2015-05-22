#!/usr/bin/env python
"""
code to delete all the association tables 
(whose primary key is not a foreign key of some table)
"""

def _deleteTableByBlockList(dbConn, blockInfoList, tableName, columnName):
    """
    _deletBlockParentageByRecoBlock_
    
    generic delete function using block ids
    """
    
    if type(blockInfoList) != list:
        blockInfoList = [blockInfoList]
    
    bindVarIndex = 0
    params = {}
    sqlQuery = """ DELETE FROM %s WHERE %s in (""" % (tableName, columnName)    
        
    for blockInfo in blockInfoList:
        bindVarIndex = bindVarIndex + 1
        sqlQuery = sqlQuery + ":p_%s, " % bindVarIndex
        params['p_%s' % bindVarIndex] = blockInfo["BLOCK_ID"]
    
    sqlQuery = sqlQuery.rstrip(", ") + ")"
    
    dbConn.execute(sqlQuery, params)
    
    
def _deleteTableByFileList(dbConn, t0astFileList, tableName, columnName):
    """
    _deleteTableByFileList_
    
    generic delete function using file ids
    """
    
    if type(t0astFileList) != list:
        t0astFileList = [t0astFileList]
    
    bindVarIndex = 0
    params = {}
    sqlQuery = """ DELETE FROM %s WHERE %s in (""" % (tableName, columnName)    
        
    for t0astFile in t0astFileList:
        bindVarIndex = bindVarIndex + 1
        sqlQuery = sqlQuery + ":p_%s, " % bindVarIndex
        params['p_%s' % bindVarIndex] = t0astFile["ID"]
    
    sqlQuery = sqlQuery.rstrip(", ") + ")"
    
    dbConn.execute(sqlQuery, params)
        
        
def deleteBlockParentageByRecoBlock(dbConn, recoBlockList):
    """
    _deleteBlockParentageByRecoBlock_
    
    remove the entry from block_parentage table when reco blocks are complete.
    """
    _deleteTableByBlockList(dbConn, recoBlockList, "block_parentage", 
                            "output_id")


def deleteBlockRunAssocByBlock(dbConn, blockList):
    """
    _deleteBlockRunAssocByBlock_
    
    remove the entry from block_run_assoc table when blocks are complete.
    """
    _deleteTableByBlockList(dbConn, recoBlockList, "block_run_assoc", 
                            "block_id")
    

def deleteRepackedRecoParentageByReco(dbConn, recoFileList):
    """
    _deleteRepackedRecoParentageByReco_
    
    remove the entry from block_run_assoc table when blocks are complete.
    """
    _deleteTableByFileList(dbConn, recoFileList, "repacked_reco_parentage", 
                            "output_id")


def deleteRecoMergeParentageByReco(dbConn, recoFileList):
    """
    _deleteRecoMergeParentageByReco_
    
    remove the entry from block_run_assoc table when blocks are complete.
    """
    _deleteTableByFileList(dbConn, recoFileList, "reco_merge_parentage", 
                            "output_id")


# need to delete hlt debug information before start to delete repacked files
          
def deleteRepackedMergeParentage(dbConn, repackedFileList):
    """
    _deleteRecoMergeParentageByReco_
    
    remove the entry from repacked_merge_parentage table when blocks are complete.
    """
    _deleteTableByFileList(dbConn, repackedFileList, "repacked_merge_parentage", 
                            "output_id")


# The methods below should get the list of corresponding id to delete further
def removeRepackStreamerAssoc(dbConn, repackedFileList):
    """
    _removeRepackStreamerAssoc_
    
    remove the entry from repack_streamer_assoc table when blocks are complete.
    and return list of streamer IDs
    """
    _deleteTableByFileList(dbConn, repackedFileList, "repack_streamer_assoc", 
                            "repacked_id")
    
    #select streamer_id from repack_streamer_assoc where repacked_id = 
    return []

# The methods below should get the list of corresponding id to delete further
def removeRepackLumiAssoc(dbConn, repackedFileList):
    """
    _removeRepackLumiAssoc_
    
    remove the entry from repack_streamer_assoc table when blocks are complete.
    and return list of streamer IDs
    """
    _deleteTableByFileList(dbConn, repackedFileList, "repack_lumi_assoc", 
                            "repacked_id")
    
    #select lumi_id, run_id from repack_lumi_assoc where repacked_id = 
    return []


# how to get repack job id for repack job to delete
# using repack_streamer_assoc, with dataset_id from repacked, job_streamer_dataset_assoc and check the job is successful
def removeJobDatasetStreamerAssoc(dConn, job_id, dataset_id, streamer_id):
    pass


# To do: delete repack and job association table. promptreco_job_repack_assoc, merge_job_repack_assoc
# The methods below should get the list of corresponding id to delete further
def removePromptrecoJobRepackAssoc(dbConn, repackedFileList):
    """
    _removeRepackLumiAssoc_
    
    remove the entry from repack_streamer_assoc table when blocks are complete.
    and return list of streamer IDs
    """
    _deleteTableByFileList(dbConn, repackedFileList, "promptreco_job_repack_assoc", 
                            "repacked_id")
    
    #select lumi_id, run_id from repack_lumi_assoc where repacked_id = 
    # get promptrecojob_ids
    return []

# The methods below should get the list of corresponding id to delete further
def removeMergeJobRepackAssoc(dbConn, repackedFileList):
    """
    _removeRepackLumiAssoc_
    
    remove the entry from merge_job_repack_assoc table when blocks are complete.
    and return list of streamer IDs
    """
    _deleteTableByFileList(dbConn, repackedFileList, "merge_job_repack_assoc", 
                            "repacked_id")
    
    #select lumi_id, run_id from repack_lumi_assoc where repacked_id = 
    # return job_ids from merge job
    return []
# To do: delete streamer related info

# To do: delete job related info
                          

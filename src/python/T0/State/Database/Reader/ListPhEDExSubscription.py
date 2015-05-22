#!/usr/bin/env python
"""
_ListPhEDExSubscription_
Read operation related phedex_subscription and storage_node table in T0AST 
"""
import logging
from T0.DataStructs.SubscriptionPolicy import SubscriptionPolicy 
from T0.DataStructs.SubscriptionPolicy import PhEDExSubscription 

def getSubscriptionPolicy(dbConn, blockList):
    """
    _getSubscriptionPolicy_
    """
    sql = """SELECT dp.id, :p_3, ps.node_id, sn.name, priority, request_only 
               FROM phedex_subscription ps
                  INNER JOIN dataset_path dp 
                        ON (dp.primary_dataset = ps.primary_dataset_id
                            AND dp.data_tier = ps.data_tier_id)
                  INNER JOIN storage_node sn ON (ps.node_id = sn.id)
                  LEFT OUTER JOIN phedex_subscription_made psm
                        ON ( psm.dataset_path_id = dp.id
                             AND psm.node_id = sn.id )
                WHERE dp.id = :p_1 AND run_id = :p_2
                      AND psm.dataset_path_id IS NULL
          """
           
    bindVars = []
    for block in blockList:
        param = {"p_1": block["DATASET_PATH_ID"], "p_2": block["RUN_ID"],
                 "p_3": block.getDatasetPath()}
        bindVars.append(param)
    
    dbConn.execute(sql, bindVars) 
    results = dbConn.fetchall()
    
    if len(results) == 0:
        return None
    
    policy = SubscriptionPolicy()
    # what will you do with run ID.
    for row in results:
        # make a tuple (dataset_path_id, dataset_path_name)
        # make a tuple (node_id, node_name)
        logging.debug("datasetpath id %s, name %s"% (row[0], row[1]))
        logging.debug("node id %s, name %s"% (row[2], row[3]))
        
        subscription = PhEDExSubscription((row[0], row[1]), (row[2], row[3]),
                                          row[4], row[5])
        policy.addSubscription(subscription)
         
    return policy

          
def listStorageNodesByBlock(dbConn, blockInfo):
    """
    _listStorageNodesByBlock_
    
    list all the node where the given block is subscribed.
    TODO: Assumes every thing is custodial node, if the requirement changes
    join phedex_subscription table to get the information 
    """
    sqlQuery = """ SELECT DISTINCT sn.name FROM storage_node sn
                   INNER JOIN phedex_subscription_made ON (node_id=sn.id)
                   WHERE dataset_path_id = :p_1
               """
                
    bindVars = {}
    bindVars["p_1"] = blockInfo["DATASET_PATH_ID"]
    
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()
    nodes = []
    for row in results:
        nodes.append(row[0])
        
    return nodes


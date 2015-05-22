"""
_updatePhEDExSubscription_

Utilities for updating PhEDEx_Subscription tables in T0AST.
"""

__revision__ = "$"
__version__ = "$"


def insertPhEDExSubscriptionMade(dbConn, subscription):
    """
    _updatePhEDExSubscription_

    Update requester_id and status in the phedex_subscription table.  
    This will update multiple subscription if there is the same subscription in 
    different run
    """
            
    sqlQuery = """ INSERT INTO phedex_subscription_made 
                       (dataset_path_id, node_id, requester_id)
                       SELECT :p_1, :p_2, :p_3 FROM DUAL
                       WHERE NOT EXISTS (SELECT * FROM phedex_subscription_made
                            WHERE dataset_path_id = :p_1 AND node_id = :p_2)
                """
    
    bindVars = []
    for datasetPathID in subscription.getDatasetPathIDs():
        for nodeID in subscription.getNodeIDs():
            param = {"p_1": datasetPathID, "p_2": nodeID,
                     "p_3": subscription.getRequesterID()}
            bindVars.append(param)
                
    dbConn.execute(sqlQuery, bindVars)
    return
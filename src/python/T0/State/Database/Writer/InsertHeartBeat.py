#!/usr/bin/env python
"""
_InsertHeartBeat_

Functions for inserting and updateing component heart beat.
"""

__revision__ = "$"
__version__ = "$"
import time

def updateHeartBeat(dbConn, componentName):
    
    insertSql = """INSERT INTO component_heartbeat (name, last_updated) 
                    SELECT :name, :currentTime FROM DUAL 
                    WHERE NOT EXISTS (SELECT name FROM component_heartbeat 
                                      WHERE name =:name) 
                """
                
    updateSql = """UPDATE component_heartbeat SET last_updated = :currentTime
                   WHERE name = :name
                """   
    params = {'name': componentName, 'currentTime' : int(time.time())}
    dbConn.execute(insertSql, params)
    dbConn.execute(updateSql, params)
    
    return          
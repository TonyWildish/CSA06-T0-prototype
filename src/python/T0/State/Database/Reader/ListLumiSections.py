#!/usr/bin/env python
"""
_ListLumisections_
Read operation related block and active_block table in T0AST 
"""
import logging
from T0.DataStructs.LumiSection import LumiSection 

#def listLumiSectionsByFile(dbConn, t0astFile):
#    
#    """
#    _ListLumisections_ 
#    retrieve lumi section related information from lumi table
#    returns list of lumi objects in relation to given t0astFile,
#    also updates t0astFile["LUMI_LIST"]
#    
#    """
#    
#    if t0astFile["TYPE"] == "Repacked":
#        repackedIdVar = ":p_1"
#    elif t0astFile["TYPE"] == "Reconstructed":
#        repackedIdVar = """ SELECT input_id FROM repacked_reco_parentage 
#                            WHERE output_id = :p_1 """
#                            
#    # need to handle better for unknown type
#    else:
#        raise Exception
#        
#    sqlQuery = """SELECT LS.lumi_id, LS.run_id, LS.start_time FROM lumi_section LS 
#                  INNER JOIN repack_lumi_assoc RLS 
#                  ON (LS.lumi_id = RLS.lumi_id AND LS.run_id = RLS.run_id)  
#                  WHERE RLS.repacked_id IN ( %s )""" % repackedIdVar
#
#    param = {'p_1':t0astFile["ID"]}
#    dbConn.execute(sqlQuery, param)
#    lumiRows = dbConn.fetchall()
#
#    lumiList = []
#    for lumiRow in lumiRows:
#        lumi = LumiSection()        
#        lumi["LUMI_ID"] = lumiRow[0]
#        lumi["RUN_ID"] = lumiRow[1]
#        lumi["START_TIME"] = lumiRow[2]
#        lumiList.append(lumi)
#        
#    #update t0astFile
#    t0astFile["LUMI_LIST"] = lumiList
#    return lumiList

def listLumiByFile(dbConn, fileID):
    """
    _listLumiByRepacked_

    list lumi ids from streamer table corresponding to the repacked/express file
    """
    lumiQuery = """SELECT DISTINCT streamer.LUMI_ID FROM streamer INNER JOIN
                   repack_streamer_assoc USING (STREAMER_ID) WHERE
                   repack_streamer_assoc.REPACKED_ID = :p_1"""

    bindVars = {"p_1": fileID}
    dbConn.execute(lumiQuery, bindVars)
    results = dbConn.fetchall()
    lumiList = []
    for result in results:
        lumiList.append(result[0])

    return lumiList
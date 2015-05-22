#!/usr/bin/env python
"""
_ListRecoed_

Utility to list recoed files in T0AST.
"""

__revision__ = "$Id: ListRecoed.py,v 1.1 2008/12/17 15:45:46 gowdy Exp $"
__version__ = "$Revision: 1.1 $"

from T0.DataStructs.T0ASTFile import T0ASTFile

def listRecoedFilesForAlca(dbConn):
    """
    _listRecoedFilesForAlca_

    Retrieve a list of recoed files in T0AST that are ready to be AlcaSkimed.
    Files are returned as a list of T0ASTFile objects and include all the
    columns in the reco table plus that stream ID for the file.
    """
    sqlQuery = """SELECT RECONSTRUCTED_ID, RUN_ID, FILESIZE, CKSUM, EVENTS,
                  LFN, DATASET_ID, STATUS, dataset_run_stream_assoc.STREAM_ID
                  FROM reconstructed INNER JOIN dataset_run_stream_assoc
                  USING (RUN_ID, DATASET_ID)
                  WHERE STATUS = (SELECT ID FROM reconstructed_status
                                  WHERE STATUS = 'Reconstructed')""" 

    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()

    recoedFiles = []
    for result in results:
        recoedFile = T0ASTFile()
        recoedFile["TYPE"] = "Reconstructed"
        recoedFile["RECOED_ID"] = result[0]
        recoedFile["ID"] = result[0]        
        recoedFile["RUN_ID"] = result[1]
        recoedFile["FILESIZE"] = result[2]
        recoedFile["CKSUM"] = result[3]
        recoedFile["EVENTS"] = result[4]
        recoedFile["LFN"] = result[5]
        recoedFile["DATASET_ID"] = result[6]
        recoedFile["STATUS"] = result[7]
        recoedFile["STREAM_ID"] = result[8]        

        recoedFiles.append(recoedFile)

    return recoedFiles

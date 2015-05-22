#!/usr/bin/env python
"""
_ListTier0ConfigValidation_

Utilities for retrieving RunConfig information from T0AST.
"""

__revision__ = "$Id: ListTier0ConfigValidation.py,v 1.1 2009/02/09 14:57:55 sryu Exp $"
__version__ = "$Revision: 1.1 $"

def listStreams(dbConn, runNumber):
    """
    _listStreams_

    Query T0AST for stream names for this run.
    """
    sqlQuery = """SELECT stream.NAME FROM stream INNER JOIN
                  dataset_run_stream_assoc ON stream.ID =
                  dataset_run_stream_assoc.stream_id
                  WHERE dataset_run_stream_assoc.run_id = :p_1"""

    bindVars = {"p_1": runNumber}
    dbConn.execute(sqlQuery, bindVars)

    # rows with 1st element the stream name, 2nd the stream id
    # now convert into dictionary
    streams = []
    for row in dbConn.fetchall():
        streams.append(row[0])

    return streams

def listDatasets(dbConn, runNumber, streamName):
    """
    _listDatasets_

    Retrieve a list of all the primary datasets belong to given run and stream in T0AST.
    """
    
    sqlQuery = """ SELECT DISTINCT pd.name FROM primary_dataset pd 
                   INNER JOIN dataset_run_stream_assoc USING (dataset_id)
                   INNER JOIN stream ON (stream.id = stream_id)
                   WHERE run_id = :p_1 and stream.name = :p_2      
               """
    bindVars = {"p_1": runNumber, "p_2":streamName}
               
    dbConn.execute(sqlQuery, bindVars)
    results = dbConn.fetchall()

    datasets = []
    for result in results:
        datasets.append(result[0])

    return datasets

def listScenarios(dbConn):
    """
    _listScenarios_
    """
    sqlQuery = """ SELECT name FROM event_scenario """
    
    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()
    scenarios = []
    for result in results:
        scenarios.append(result[0])

    return scenarios

def listCMSSWVersion(dbConn):
    """
    _listScenarios_
    """
    sqlQuery = """ SELECT name FROM cmssw_version """
    
    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()
    versions = []
    for result in results:
        versions.append(result[0])

    return versions
#!/usr/bin/env python
"""
_RunLumiCloseout_

Methods to closeout runs and lumi sections
"""

__revision__ = "$Id: RunLumiCloseout.py,v 1.9 2009/07/06 19:49:31 hufnagel Exp $"
__version__ = "$Revision: 1.9 $"

import time
import logging


def closeLumiSections(dbConn, timeout):
    """
    _closeLumiSection_

    close lumi sections where all streamers are older than timeout
    
    """
    sqlQuery = """SELECT a.RUN_ID, a.LUMI_ID, a.STREAM_ID from streamer a
                  LEFT OUTER JOIN lumi_section_closed b
                  ON b.RUN_ID = a.RUN_ID AND b.LUMI_ID = a.LUMI_ID AND b.STREAM_ID = a.STREAM_ID
                  WHERE b.RUN_ID IS NULL
                  GROUP BY a.RUN_ID, a.LUMI_ID, a.STREAM_ID"""

    dbConn.execute(sqlQuery)
    results = dbConn.fetchall()

    currentTime = int(time.time())
    lumiSectionsToClose = []
    for result in results:

        bindVars = { 'RUN_ID' : result[0],
                     'LUMI_ID' : result[1],
                     'STREAM_ID' : result[2],
                     'INSERT_TIME' : currentTime,
                     'TIMEOUT' : currentTime - timeout }

        lumiSectionsToClose.append(bindVars)

    if len(lumiSectionsToClose) > 0:

        sqlQuery = """INSERT INTO lumi_section_closed (RUN_ID, LUMI_ID, STREAM_ID, INSERT_TIME)
                      SELECT :RUN_ID, :LUMI_ID, :STREAM_ID, :INSERT_TIME FROM DUAL
                      WHERE
                       (
                        SELECT MAX(a.INSERT_TIME) from streamer a
                        WHERE a.RUN_ID = :RUN_ID
                        AND a.LUMI_ID = :LUMI_ID
                        AND a.STREAM_ID = :STREAM_ID
                       ) < :TIMEOUT"""

        dbConn.execute(sqlQuery, lumiSectionsToClose)

    return


def expressDoneLumiSections(dbConn):
    """
    _expressDoneForLumiSection_

    mark lumi sections as express done if

      they are closed
      all files in them have express jobs
      all these express jobs have succeeded

    """
    sqlQuery = """INSERT INTO lumi_section_express_done (RUN_ID, LUMI_ID, STREAM_ID, INSERT_TIME)
                  SELECT a.RUN_ID, a.LUMI_ID, a.STREAM_ID, :INSERT_TIME from lumi_section_closed a
                  LEFT OUTER JOIN lumi_section_express_done b
                  ON b.RUN_ID = a.RUN_ID AND b.LUMI_ID = a.LUMI_ID AND b.STREAM_ID = a.STREAM_ID
                  WHERE b.RUN_ID IS NULL
                  AND NOT EXISTS
                    (
                      SELECT * FROM streamer c
                      LEFT OUTER JOIN job_streamer_assoc e
                      ON e.streamer_id = c.streamer_id
                      LEFT OUTER JOIN express_job_def f
                      ON f.job_id = e.job_id
                      INNER JOIN run_stream_style_assoc g
                      ON g.run_id = c.run_id AND g.stream_id = c.stream_id
                      INNER JOIN processing_style h
                      ON h.id = g.style_id
                      WHERE h.name = 'Express'
                      AND c.RUN_ID = a.RUN_ID AND c.LUMI_ID = a.LUMI_ID AND c.STREAM_ID = a.STREAM_ID
                      AND
                        ( e.streamer_id IS NULL
                          OR NOT f.job_status = (SELECT id from job_status WHERE status = 'Success') )
                    )
                  GROUP BY a.RUN_ID, a.LUMI_ID, a.STREAM_ID"""

    currentTime = int(time.time())
    bindParams = { 'INSERT_TIME' : currentTime }

    dbConn.execute(sqlQuery, bindParams)

    return


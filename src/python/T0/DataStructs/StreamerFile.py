#!/usr/bin/env python
"""
_StreamerFile_

Data Object representing a streamer table and the information
related to it

"""

class StreamerFile(dict):
    """
    _StreamerFile_

    Dict based object for containing information about a streamer table
    
    i.e.) Streamer table in T0AST
    streamer_id     int           not null,
    run_id          int           not null,
    lumi_id         int           not null,
    insert_time     int           not null,
    filesize        int           not null,
    events          int           not null,
    lfn             varchar(1000) unique not null,
    exportable      int           not null,
    stream_id       int,
    indexpfn        varchar(1000),
    indexpfnbackup  varchar(1000),
    
    """
    def __init__(self, **args):
        dict.__init__(self)
        self.setdefault("ID", None)
        self.setdefault("RunNumber", None)       
        self.setdefault("LumiID", None)
        self.setdefault("InsertTime", None)          
        self.setdefault("FileSize", None)
        self.setdefault("Events", None)
        self.setdefault("LFN", None)          
        self.setdefault("Stream", None)
        self.setdefault("Exportable", 0)
        self.setdefault("IndexPFN", None)
        self.setdefault("IndexPFNBackup", None)
        self.update(args)

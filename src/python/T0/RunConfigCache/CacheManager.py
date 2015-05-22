#!/usr/bin/env python
"""
_CacheMgr_

Maintain a module level instance of the RunConfig Cache so that the
same instance can be accessed from different places within the same
process without having to track a reference.

TODO: Provide a threadsafe manager for the cache so that multithreaded
components can use it.

"""


from T0.RunConfigCache.Cache import Cache
from T0.RunConfigCache.FileBackend import FileBackend

class Manager:
    """
    Namespace like Container for Cache instance

    """
    _Cache = Cache()


def getRunConfigCache(t0astDBConn = None, filename = None):
    """
    _getRunConfigCache_

    Get ref to the Cache instance

    Optionally, can use a FileBackend instead of talking to RunSummary/ConfDB

    """
    if filename != None:
        Manager._Cache.fileBackend = FileBackend(filename)
    
    if t0astDBConn != None:
        Manager._Cache.t0astDBConn = t0astDBConn
          
    return Manager._Cache

#!/usr/bin/env python
"""
_RunConfigCacheError_

Exception definitions for RunConfigCache package.

All Exceptions to inherit RunConfigCacheError to allow easy
error handling for other packages

"""


from ProdCommon.Core.ProdException import ProdException


class RunConfigCacheError(ProdException):
    """
    _RunConfigCacheError_

    Basic RunConfigCache error

    """
    def __init__(self, msg, **data):
        ProdException.__init__(self, msg, errorNo=2000, **data)



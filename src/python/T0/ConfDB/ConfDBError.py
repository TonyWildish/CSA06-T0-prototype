#!/usr/bin/env python
"""
_ConfDBError_

Exception definitions for ConfDB package.

All Exceptions to inherit ConfDBError to allow easy
error handling for other packages

"""


from ProdCommon.Core.ProdException import ProdException


class ConfDBError(ProdException):
    """
    _ConfDBError_

    Basic ConfDB error

    """
    def __init__(self, msg, **data):
        ProdException.__init__(self, msg, errorNo=3000, **data)



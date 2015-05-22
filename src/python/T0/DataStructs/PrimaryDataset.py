#!/usr/bin/env python
"""
_PrimaryDataset_

Object representing a PrimaryDataset

"""

from T0.DataStructs.TriggerPath import TriggerPath
from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery


class PrimaryDataset(dict):
    """
    _PrimaryDataset_

    Object representing a PrimaryDataset in terms of the
    trigger paths it is comprised of

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("PrimaryName", None)
        self.setdefault("RepackAlgorithm", None)
        self.triggerPaths = {}

    def addTriggerPath(self, pathname, selectEvents = None):
        """
        _addTriggerPath_

        Add a new TriggerPath to this dataset

        """
        newPath = TriggerPath()
        newPath['Name'] = pathname
        newPath['SelectEvents'] = selectEvents
        self.triggerPaths[pathname] = newPath
        return
        
    def save(self):
        """
        _save_

        convert this object to an improv Node

        """
        result = IMProvNode("PrimaryDataset")
        result.attrs['PrimaryName'] = self['PrimaryName']
        result.attrs['RepackAlgorithm'] = self['RepackAlgorithm']
        for tpath in self.triggerPaths.values():
            result.addNode(tpath.save())
        return result


    def load(self, improvNode):
        """
        _load_

        Populate self with contents of improvNode

        """
        self['PrimaryName'] = improvNode.attrs.get("PrimaryName", None)
        self['RepackAlgorithm'] = improvNode.attrs.get("RepackAlgorithm", None)
        tpathQ = IMProvQuery("/PrimaryDataset/TriggerPath")
        tpaths = tpathQ(improvNode)
        for tpath in tpaths:
            newPath = TriggerPath()
            newPath.load(tpath)
            self.triggerPaths[newPath['Name']] = newPath
        return

    

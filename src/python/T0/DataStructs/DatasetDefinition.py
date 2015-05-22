#!/usr/bin/env python
"""
_DatasetDefinition_

Object representing a PrimaryDataset and related information

stream : Stream Name (express, calib, physics)
dataset : Primary dataset name
triggerPaths : list of trigger paths belong to the dataset

"""

class DatasetDefinition(dict):
    """
    _DatasetDefinition_
    
    Object representing a PrimaryDataset and related information
    
    stream : Stream Name (express, calib, physics)
    dataset : Primary dataset name
    process : Name of HLT Process, needed for SelectEvents
    triggerPaths : list of trigger paths belong to the dataset
    
    """
    def __init__(self, **args):
        dict.__init__(self)
        self.setdefault("stream", None)
        self.setdefault("dataset", None)
        self.setdefault("process", None)
        self.setdefault("processedDataset", None)
        self.setdefault("globalTag", None)
        self.setdefault("acquisitionEra", None)
        self.setdefault("processingVersion",None)
        self.update(args)

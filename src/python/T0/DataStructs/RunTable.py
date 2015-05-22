#!/usr/bin/env python
"""
_RunTable_

Details of a run in terms of primary datasets, trigger paths
and select events.

"""

from T0.DataStructs.PrimaryDataset import PrimaryDataset

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvFile
    

class RunTable(dict):
    """
    _RunTable_

    Dictionary based object that contains details
    of the primary dataset and trigger path splits
    for a given run

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("RunNumber")
        self.primaryDatasets = {}


    def addPrimaryDataset(self, datasetName, repackAlgo = "Split"):
        """
        _addPrimaryDataset_

        Create a new primary dataset for this run.
        Returns a reference to the instance of PrimaryDataset added
        so that trigger paths can be added to it

        """
        if self.primaryDatasets.has_key(datasetName):
            return self.primaryDatasets[datasetName]
        newDataset = PrimaryDataset()
        newDataset['PrimaryName'] = datasetName
        newDataset['RepackAlgorithm'] = repackAlgo
        self.primaryDatasets[datasetName] = newDataset
        return newDataset

    def __str__(self):
        """string rep of this object for debugging"""
        return str(self.save())

    def save(self):
        """
        _save_

        Convert this instance into an IMProv structure

        """
        result = IMProvNode("RunTable")
        result.attrs['RunNumber'] = self['RunNumber']
        for dataset in self.primaryDatasets.values():
            result.addNode(dataset.save())
        return result
                            
    def load(self, improvNode):
        """
        _load_

        Load data into self from improvNode

        """
        self['RunNumber'] = int(improvNode.attrs.get("RunNumber", None))
        datasetQ = IMProvQuery("/RunTable/PrimaryDataset")
        datasets = datasetQ(improvNode)
        for dataset in datasets:
            newDataset = PrimaryDataset()
            newDataset.load(dataset)
            self.primaryDatasets[newDataset['PrimaryName']] = newDataset
        return
                               

def persistRuns(filename, *runTables):
    """
    _persistRuns_

    Util to save a list of RunTable instances to a file

    """
    topNode = IMProvNode("Runs")
    [ topNode.addNode(x.save()) for x in runTables ]

    handle = open(filename, 'w')
    handle.write(topNode.makeDOMElement().toprettyxml())
    handle.close()
    return

def loadRunsFile(filename):
    """
    _loadRunsFile_

    Load a file containing a set of run tables as XML
    Returns a list of RunTable instances
    
    """
    improvNode = loadIMProvFile(filename)
    runTableQ = IMProvQuery("RunTable")
    runTables = runTableQ(improvNode)
    result = []
    for runTable in runTables:
        newTable = RunTable()
        newTable.load(runTable)
        result.append(newTable)
    return result
    

        
if __name__ == '__main__':
    run = RunTable()
    run['RunNumber'] = 100001
    ds1 = run.addPrimaryDataset("primary1", "Split")
    ds2 = run.addPrimaryDataset("primary2", "Accumulate")
    
    ds1.addTriggerPath("p1", "HLT:p1")
    ds1.addTriggerPath("p2", "HLT:p2")
    ds2.addTriggerPath("p3", "HLT:p3")
    ds2.addTriggerPath("p4", "HLT:p4")
    
    #print str(run)
        
    improv = run.save()
    run2 = RunTable()
    run2.load(improv)

    persistRuns("test.xml", run, run2)
    
    runs = loadRunsFile("test.xml")
    for r in runs:
        print str(r)

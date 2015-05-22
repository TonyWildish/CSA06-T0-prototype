

import time
import hotshot.stats
from ProdCommon.Database import Session

import T0.State.Database.Writer.InsertRun as InsertRun
import T0.State.Database.Writer.InsertLumi as InsertLumi
import T0.State.Database.Writer.InsertTrigger as InsertTrigger

SingleRun = 1000001
ThousandRuns = [ 1000000 + i for i in range(0, 1000) ]
LumiSections = [0,1,2,3,4,5]

Triggers = {}
[ Triggers.__setitem__("trigger%s" % x, "dataset%s" % x)
      for x in range(0, 50) ]


def printProfileSummary(prof):
    """
    _printProfileSummary_

    Dump profile summary from file provided

    """
    
    stats = hotshot.stats.load(prof)
    stats.strip_dirs()
    stats.sort_stats('time', 'calls')
    stats.print_stats(10)
    return

def create1Run():
    """
    _create1Run_

    Create a single run entry for testing

    """
    InsertRun.insertRun(SingleRun)
    Session.commit_all()
    return SingleRun

def delete1Run():
    """
    _delete1Run_

    Delete the single run entry

    """
    InsertRun.deleteRun(SingleRun)
    Session.commit_all()
    return

def create1000Runs():
    """
    _create1000Runs_

    Create 1k test run entries

    """
    
    [ InsertRun.insertRun(x) for x in ThousandRuns ]
    Session.commit_all()
    return ThousandRuns


def delete1000Runs():
    """
    _delete1000Runs_

    Delete the thousand run entries

    """
    [ InsertRun.deleteRun(x) for x in ThousandRuns ]
    Session.commit_all()
    return

def createLumis(run):
    """
    _createLumis_

    Create 0-6 Lumi sections for the run number provided

    """
    for i in LumiSections:
        InsertLumi.insertLumi(run, i)
    Session.commit_all()
    return LumiSections

def createTriggers():
    """
    _createTriggers_

    Create triggers0-50 with datasets0-50

    """
    ids = InsertTrigger.insertTriggers(**Triggers)
    Session.commit_all()
    return ids

def deleteTriggers():
    """
    _deleteTriggers_

    remove test triggers

    """
    InsertTrigger.deleteTriggers(*Triggers.keys())
    Session.commit_all()
    return


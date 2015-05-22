#!/usr/bin/env/python
"""
_installT0Schema_

Install the T0 schema into the given oracle instance.
"""

import commands
import sys
import logging
import threading
import getopt

from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription

from WMCore.DAOFactory import DAOFactory
from WMCore.Database import DBFactory

from ProdAgentCore.Configuration import loadProdAgentConfiguration

def clearDBInstance(dbName):
    """
    _clearDBInstance_

    Run to OracleReset.sql script in T0/src/sql to clear out the database
    instance before we try to load in the schema.
    """
    print "Clearing the DB instance..."

    clearCommand = "rm %s" % dbName
    result = commands.getstatusoutput(clearCommand)
    
    if result[0] != 0:
        print "An error occured: %s" % result[1]

    return

def installWMBS(dbConn, inter):
    """
    _installWMBS_

    Install WMBS into the newly cleared database instance.
    """
    print "Installing WMBS..."
    
    daoFactory = DAOFactory(package="WMCore.WMBS", logger = logging,
                            dbinterface = inter )
    daoFactory(classname = "Create").execute()
    return

def setupWMBS():
    """
    _setupWMBS_

    Create the initial workflows, subscriptions and filesets.

    Subscriptions:
        Mergeable
        Exportable
        Reconstructable
        
    """
    print "Creating WMBS workflows, filesets and subscriptions."
    
    expressMergeWorkflow = Workflow(spec = "ExpressFileMerge", owner = "CMSTier0",
                                 name = "ExpressFileMerge")
    mergeWorkflow = Workflow(spec = "FileMerge", owner = "CMSTier0",
                             name = "FileMerge")
    expressDBSWorkflow = Workflow(spec = "ExpressDBSUpload", owner = "CMSTier0",
                                     name = "ExpressDBSUpload")
    expressTransferWorkflow = Workflow(spec = "ExpressTransfer", owner = "CMSTier0",
                                     name = "ExpressTransfer")
    dbsUploadWorkflow = Workflow(spec = "FileDBSUpload", owner = "CMSTier0",
                              name = "FileDBSUpload")
    transferWorkflow = Workflow(spec = "FileTransfer", owner = "CMSTier0",
                            name = "FileTransfer")    
    recoWorkflow = Workflow(spec = "FileReconstruction", owner = "CMSTier0",
                            name = "FileReconstruction")
    alcaWorkflow = Workflow(spec = "FileAlcaSkim", owner = "CMSTier0",
                            name = "FileAlcaSkim")
    deleteWorkflow = Workflow(spec = "FileDelete", owner = "CMSTier0",
                            name = "FileDelete") 
    wmbsPublishWorkflow = Workflow(spec = "FileWMBSPublish",
                                   owner = "CMSTier0",
                                   name = "FileWMBSPublish") 
    
    expressMergeWorkflow.create()
    mergeWorkflow.create()
    expressDBSWorkflow.create()
    expressTransferWorkflow.create()
    dbsUploadWorkflow.create()
    recoWorkflow.create()
    transferWorkflow.create()
    alcaWorkflow.create()
    deleteWorkflow.create()
    wmbsPublishWorkflow.create()

    expressMergeFileset = Fileset(name = "ExpressMergeable")
    mergeFileset = Fileset(name = "Mergeable")
    expressDBSFileset = Fileset(name = "ExpressDBSUploadable")
    expressTransferFileset = Fileset(name = "ExpressTransferable")
    dbsUploadFileset = Fileset(name = "DBSUploadable")
    transferFileset = Fileset(name = "Transferable")
    recoFileset = Fileset(name = "Reconstructable")
    alcaFileset = Fileset(name = "AlcaSkimmable")
    deleteFileset = Fileset(name = "Deletable")
    wmbsPublishFileset = Fileset(name = "WMBSPublishable")
    
    expressMergeFileset.create()
    mergeFileset.create()
    expressDBSFileset.create()
    expressTransferFileset.create() 
    dbsUploadFileset.create()
    transferFileset.create()
    recoFileset.create()
    alcaFileset.create()
    deleteFileset.create()
    wmbsPublishFileset.create()

    mergepackSubscription = Subscription(fileset = expressMergeFileset, workflow = expressMergeWorkflow)
    mergeSubscription = Subscription(fileset = mergeFileset, workflow = mergeWorkflow)
    expressDBSSubscription = Subscription(fileset = expressDBSFileset, workflow = expressDBSWorkflow)
    expressTransferSubscription = Subscription(fileset = expressTransferFileset, workflow = expressTransferWorkflow)
    dbsUploadSubscription = Subscription(fileset = dbsUploadFileset, workflow = dbsUploadWorkflow)
    transferSubscription = Subscription(fileset = transferFileset, workflow = transferWorkflow)
    recoSubscription = Subscription(fileset = recoFileset, workflow = recoWorkflow, split_algo = "FileAndEventBased")
    alcaSubscription = Subscription(fileset = alcaFileset, workflow = alcaWorkflow, split_algo = "FileBased")
    deleteSubscription = Subscription(fileset = deleteFileset, workflow = deleteWorkflow)
    wmbsPublishSubscription = Subscription(fileset = wmbsPublishFileset, workflow = wmbsPublishWorkflow)

    mergepackSubscription.create()
    mergeSubscription.create()
    expressDBSSubscription.create()
    expressTransferSubscription.create()
    dbsUploadSubscription.create()
    transferSubscription.create()
    recoSubscription.create()
    alcaSubscription.create()
    deleteSubscription.create()
    wmbsPublishSubscription.create()
    
    return

def usage():
    print "%s: [-h] [dataBaseInstance]" % sys.argv[0]
    print
    print """Default database instance is PromptCalibDB. In all cases
the database must be defined in the ProdAgent configuration
file."""

def main( argv ):
    dbName = "PromptCalibDB"
    try:
        opts, args = getopt.getopt(argv, "h", ["help"])
    except getopt.GetoptError:
        print "Unable to parse argument list"
        usage()
        sys.exit(2)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()

    if len(args) > 1:
        print "Only expected one non-option argument"
        usage()
        sys.exit(2)
    elif len(args) == 1:
        dbName = args[0]

    paConfig = loadProdAgentConfiguration()
    t0astDBConfig = paConfig.getConfig( dbName )

    print "Going to install the WMBS schema:"
    print "  File: %s" % t0astDBConfig["dbName"]
    print ""
    print "Is this OK? (yes/no): ",

    line = sys.stdin.readline()
    if line != "yes\n":
        sys.exit(0)

    print ""

    clearDBInstance(t0astDBConfig["dbName"])

    t0astDBConn = DBFactory.DBFactory(logging,"sqlite:///"+ t0astDBConfig['dbName'])
    dbInterface = t0astDBConn.connect()

    myThread = threading.currentThread()
    myThread.dialect = "sqlite"
    myThread.dbi = dbInterface
    myThread.logger = logging

    installWMBS(t0astDBConn, dbInterface)
    setupWMBS()

    sys.exit(0)

if __name__ == "__main__":
    main( sys.argv[1:] )

#!/usr/bin/env/python
"""
_installT0Schema_

Install the T0 schema into the given oracle instance.
"""

import commands
import sys
import logging
import threading

from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription

from T0.GenericTier0 import Tier0DB
from WMCore.DAOFactory import DAOFactory

from ProdAgentCore.Configuration import loadProdAgentConfiguration

def clearDBInstance(userName, password, server):
    """
    _clearDBInstance_

    Run to OracleReset.sql script in T0/src/sql to clear out the database
    instance before we try to load in the schema.
    """
    print "Clearing the DB instance..."

    clearCommand = "sqlplus %s/%s@%s < ../src/sql/OracleReset.sql" % (userName,
                                                                      password,
                                                                      server)
    
    result = commands.getstatusoutput(clearCommand)
    
    if result[0] != 0:
        print "An error occured: %s" % result[1]

    return

def installWMBS(dbConn):
    """
    _installWMBS_

    Install WMBS into the newly cleared database instance.
    """
    print "Installing WMBS..."
    
    daoFactory = DAOFactory(package="WMCore.WMBS", logger = logging,
                            dbinterface = dbConn.getDBInterface())
    daoFactory(classname = "Create").execute()
    return

def installT0AST(userName, password, server):
    """
    _installT0AST_

    The T0AST schema is installed ontop of a WMBS schema.
    """
    print "Installing T0AST..."

    t0Install = "sqlplus %s/%s@%s < ../src/sql/TOAST_Oracle.sql" % (userName,
                                                                    password,
                                                                    server)
    
    result = commands.getstatusoutput(t0Install)

##    if result[0] != 0:
##        print "An error occured: %s" % result[1]

    print "%s" % result[1]

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
    combinedAlcaFileset = Fileset(name = "CombinedAlcaSkimmable")
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
    combinedAlcaFileset.create()
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
    combinedAlcaSubscription = Subscription(fileset = combinedAlcaFileset, workflow = alcaWorkflow, split_algo = "SplitFileBased")
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
    combinedAlcaSubscription.create()
    deleteSubscription.create()
    wmbsPublishSubscription.create()
    
    return

if __name__ == "__main__":
    paConfig = loadProdAgentConfiguration()
    t0astDBConfig = paConfig.getConfig("Tier0DB")

    print "Going to install the T0AST schema:"
    print "  Username: %s" % t0astDBConfig["user"]
    print "  TNS Name: %s" % t0astDBConfig["tnsName"]
    print ""
    print "Is this OK? (yes/no): ",

    line = sys.stdin.readline()
    if line != "yes\n":
        sys.exit(0)

    print ""

    clearDBInstance(t0astDBConfig["user"], t0astDBConfig["passwd"],
                    t0astDBConfig["tnsName"])

    t0astDBConn = Tier0DB.Tier0DB(t0astDBConfig)
    t0astDBConn.connect()

    myThread = threading.currentThread()
    myThread.dialect = "Oracle"
    myThread.dbi = t0astDBConn.getDBInterface()
    myThread.logger = logging

    installWMBS(t0astDBConn)
    installT0AST(t0astDBConfig["user"], t0astDBConfig["passwd"],
                 t0astDBConfig["tnsName"])
    setupWMBS()

    t0astDBConn.commit()
    t0astDBConn.close()
    sys.exit(0)

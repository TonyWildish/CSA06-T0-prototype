#!/usr/bin/env python
"""
_Tier0DB_

Wrapper class for database operations on T0AST.
"""

__revision__ = "$Id: Tier0DB.py,v 1.11 2009/07/16 21:16:45 sfoulkes Exp $"
__version__ = "$Revision: 1.11 $"

import logging
import threading

from WMCore.Database import DBFactory
from WMCore.Database import DBFormatter
from WMCore.Database import Transaction

class Tier0DB:
    """
    _Tier0DB_
    
    Wrapper class for database operations on T0AST.
    """
    def __init__(self, connectionParameters, manageGlobal = False):
        """
        ___init___

        Create the WMCore DBInterface object and setup some other data
        members.
        """
        self.dbTransaction = None
        self.dbInterface = None
        self.dbFormatter = None
        self.queryResult = None
        self.manageGlobal = manageGlobal

        self.connectionParams = {}
        if connectionParameters.get("dbType", None) == "oracle":
            self.connectionParams["dialect"] = connectionParameters["dbType"]
            self.connectionParams["passwd"] = connectionParameters["passwd"]
            self.connectionParams["user"] = connectionParameters["user"]
            self.connectionParams["tnsName"] = connectionParameters["tnsName"]
        else:
            self.connectionParams["dialect"] = "mysql"
            self.connectionParams["unix_socket"] = connectionParameters["socketFileLocation"]
            self.connectionParams["passwd"] = connectionParameters["passwd"]
            self.connectionParams["host"] = connectionParameters["host"]
            self.connectionParams["user"] = connectionParameters["user"]
            self.connectionParams["database"] = connectionParameters["dbName"]            

        self.dbFactory = DBFactory.DBFactory(logging, None,
                                             self.connectionParams)
        return

    def connect(self):
        """
        _connect_

        Connect to the database.
        """
        self.dbInterface = self.dbFactory.connect()
        self.dbFormatter = DBFormatter.DBFormatter(logging, self.dbInterface)

        if self.manageGlobal == True:
            myThread = threading.currentThread()            
            myThread.dbi = self.dbInterface
            myThread.transaction = Transaction.Transaction(self.dbInterface)
            myThread.logger = logging
            myThread.dialect = "oracle"
        else:
            self.dbTransaction = Transaction.Transaction(self.dbInterface)
        
        return

    def commit(self):
        """
        _commit_

        Commit a database transaction.  A transaction is started on the first
        call to execute().  
        """
        if self.manageGlobal == True:
            myThread = threading.currentThread()
            if myThread.transaction.conn != None:
                myThread.transaction.commit()
        else:
            if self.dbTransaction.conn != None:
                self.dbTransaction.commit()

        return

    def beginTransaction(self):
        """
        _beginTransaction_

        Begin a transaction.  If one is already active nothing will be done.
        """
        if self.manageGlobal == True:
            myThread = threading.currentThread()

            if myThread.transaction == None:
                myThread.transaction = Transaction.Transaction(self.dbInterface)
                
            if myThread.transaction.conn == None:
                myThread.transaction.begin()
        else:
            if self.dbTransaction == None:
                self.dbTransaction = Transaction.Transaction(self.dbInterface)
                
            if self.dbTransaction.conn == None:
                self.dbTransaction.begin()

        return
        
    def execute(self, sqlQuery, bindVars = None):
        """
        _execute_

        Execute a sql statement.  If this is the first query that has been run
        with the object or the first query after a commit a new transaction
        will be started.
        """
        self.beginTransaction()

        if self.manageGlobal == True:
            myThread = threading.currentThread()            
            self.queryResult = myThread.dbi.processData(sqlQuery, bindVars,
                                                        conn = myThread.transaction.conn,
                                                        transaction = True)
        else:
            self.queryResult = self.dbInterface.processData(sqlQuery, bindVars,
                                                            conn = self.dbTransaction.conn,
                                                            transaction = True)

        return

    def getDBInterface(self):
        """
        _getDBInterface_

        Retrieve the WMCore dbInterface object.  
        """
        return self.dbInterface

    def getTransaction(self):
        """
        _getTransaction_

        Retrieve a reference to the WMCore Transaction object.
        """
        return self.dbTransaction

    def getDBFactory(self):
        """
        _getDBFactory_

        Retrieve the WMCore dbFactory object.
        """
        return self.dbFactory

    def fetchall(self):
        """
        _fetchall_

        Fetch all the rows that were returned from a query.
        """
        return self.dbFormatter.format(self.queryResult)

    def rollback(self):
        """
        _rollback_

        Rollback the current transaction.
        """
        if self.manageGlobal:
            myThread = threading.currentThread()

            if myThread.transaction == None:
                logging.error("No active transaction, nothing to roll back.")
                return
            
            if myThread.transaction.conn != None:
                myThread.transaction.rollback()
        else:
            if self.dbTransaction == None:
                logging.error("No active transaction, nothing to roll back.")
                return
            
            if self.dbTransaction.conn != None:
                self.dbTransaction.rollback()

        return

    def close(self):
        """
        _close_

        Close the database connection.
        """
        return

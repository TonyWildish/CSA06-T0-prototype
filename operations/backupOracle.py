#!/usr/bin/env/python
"""
_installT0Schema_

Install the T0 schema into the given oracle instance.
"""

import commands
import sys
import os
import logging
import threading

from T0.GenericTier0 import Tier0DB
from WMCore.DAOFactory import DAOFactory
from WMCore.Database import DBFactory

from ProdAgentCore.Configuration import loadProdAgentConfiguration

def determineSchema(dbConn):
    """
    _determineSchema_

    Use the meta data maintained by Oracle to determine the schema.  The data
    is returned as a dictionary keyed by tablename.  The values are a list of
    columns where each column is a dictionary with information about the column.
    """
    sqlQuery = "SELECT tname, colno, cname, coltype, width FROM col"
    dbConn.execute(sqlQuery)

    results = dbConn.fetchall()

    tableInfo = {}
    for result in results:
        if result[0] not in tableInfo.keys():
            tableInfo[result[0]] = []

        newColumn = {}
        newColumn["NUMBER"] = int(result[1])
        newColumn["NAME"] = result[2]
        newColumn["TYPE"] = result[3]
        newColumn["WIDTH"] = result[4]

        tableInfo[result[0]].append(newColumn)

    return tableInfo

def compareColumns(a, b):
    """
    _compareColumns_

    Compare two columns by their column number.
    """
    if a["NUMBER"] > b["NUMBER"]:
        return 1
    elif a["NUMBER"] == b["NUMBER"]:
        return 0
    else:
        return -1

def createSchema(sqliteDBI, tableInfo):
    """
    _createSchema_

    Create the Oracle schema in a SQLite database.
    """
    print "Creating tables:"
    
    for tableName in tableInfo.keys():
        columns = tableInfo[tableName]
        columns.sort(compareColumns)

        print "  %s" % tableName

        createString = "CREATE TABLE %s (" % tableName.lower()

        for column in columns:
            if column["TYPE"] == "NUMBER":
                colStr = "%s INTEGER," % column["NAME"].lower()
            elif column["TYPE"] == "VARCHAR2":
                colStr = "%s VARCHAR(%s)," % (column["NAME"].lower(), column["WIDTH"])
            elif column["TYPE"] == "CHAR":
                colStr = "%s VARCHAR(%s)," % (column["NAME"].lower(), column["WIDTH"])                
            else:
                print "Unknown column type: %s" % column["TYPE"]
                colStr = None
                
            createString += colStr

        createString = createString[:-1] + ")"
        sqliteDBI.processData(createString, None, None, None)

    print ""
    print ""
    return

def copyData(t0astDBConn, sqliteDBI, tableInfo):
    """
    _copyData_

    Copy data between the Oracle and SQLite databases.
    """
    print "Copying data:"
    
    for tableName in tableInfo.keys():
        rowQuery = "SELECT count(*) FROM %s" % tableName
        t0astDBConn.execute(rowQuery)
        result = t0astDBConn.fetchall()[0][0]

        print "  %s -> %s rows" % (tableName, result)

        selectQuery = "SELECT "
        insertQuery = "INSERT INTO %s (" % tableName

        columns = tableInfo[tableName]
        columns.sort(compareColumns)
        for column in columns:
            selectQuery += "%s," % column["NAME"]
            insertQuery += "%s," % column["NAME"]

        selectQuery = selectQuery[:-1] + " FROM %s" % tableName
        insertQuery = insertQuery[:-1] + ") VALUES ("

        for i in range(len(columns)):
            insertQuery += ":p_%s," % i

        insertQuery = insertQuery[:-1] + ")"

        t0astDBConn.execute(selectQuery)
        results = t0astDBConn.fetchall()

        for result in results:
            bindVars = {}
            bindVarCounter = 0
            for col in result:
                bindVars["p_%s" % bindVarCounter] = col
                bindVarCounter += 1

            sqliteDBI.processData(insertQuery, bindVars, None, None)
        
if __name__ == "__main__":
    paConfig = loadProdAgentConfiguration()
    t0astDBConfig = paConfig.getConfig("Tier0DB")

    t0astDBConn = Tier0DB.Tier0DB(t0astDBConfig)
    t0astDBConn.connect()

    connectionParams = {}
    connectionParams["dialect"] = "sqlite"
    connectionParams["database"] = "backup.lite"

    if os.path.exists("backup.lite"):
        os.remove("backup.lite")

    dbFactory = DBFactory.DBFactory(logging, None, connectionParams)
    dbInterface = dbFactory.connect()
                                        
    tableInfo = determineSchema(t0astDBConn)
    createSchema(dbInterface, tableInfo)
    copyData(t0astDBConn, dbInterface, tableInfo)

    t0astDBConn.commit()
    t0astDBConn.close()
    sys.exit(0)

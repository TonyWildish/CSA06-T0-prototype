#!/usr/bin/env python
"""
_GenericTier0Component_

The base class for a generic Tier 0 component.  Has
methods to talk to the T0AST database as well as the
RunConfig cache.
"""

__revision__ = "$Id: GenericTier0Component.py,v 1.38 2009/06/01 15:17:34 sfoulkes Exp $"
__version__ = "$Revision: 1.38 $"

import os
import logging
import traceback
import sys
import threading

from MessageService.MessageService import MessageService
from ProdAgentCore.Configuration import loadProdAgentConfiguration
import ProdAgentCore.LoggingUtils as LoggingUtils

from WMCore.Database import Transaction

from T0.GenericTier0 import Tier0DB
from T0.RunConfigCache.CacheManager import getRunConfigCache
from T0.State.Database.Writer import InsertHeartBeat

class GenericTier0Component:
    """
    _GenericTier0Component_

    The base class for a generic Tier 0 component.  Has
    methods to talk to the T0AST database.
    """
    def __init__(self, **args):
        """
        ___init___

        Setup the logger for the component.  Also, create the database
        connection handle to the Tier0 database.
        """
        self.args = {}
        self.args['ComponentDir'] = None
        self.args['Logfile'] = None
        self.args.update(args)
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(
                self.args['ComponentDir'],
                "ComponentLog")
        LoggingUtils.installLogHandler(self)

        # Instantiate the message service and register the component with it.
        self.ms = MessageService()
        self.componentName = self.__class__.__name__.replace("Component", "")
        self.ms.registerAs(self.componentName)

        # Add heartbeat poll time
        self.heartbeat = self.lookupParameter("HeartbeatPollTime", "00:01:00")

        # Instances of the Tier0DB class that has been configured to talk to
        # T0AST and the PA DB.
        self.t0astDBConn = None
        self.paDBConn = None

        # Reference to the RunConfigCache.
        self.runConfigCache = None

    def __call__(self, event, payload):
        """
        _operator(message, payload)_

        Method called in response to this component recieving a message.  This
        should be overwritten by the subclass.
        """
        logging.error("This method shouldn't be called: event %s, payload %s" %
                      (event, payload))
        return
        
    def connectT0AST(self, manageGlobal = True):
        """
        _connectT0AST_

        Connect to the T0AST database.  

        Note that connection parameters are pulled from the "Tier0DB"
        section of the ProdAgent configuration file.
        """
        if self.t0astDBConn != None:
            return
        
        paConfig = loadProdAgentConfiguration()
        self.t0astDBConfig = paConfig.getConfig("Tier0DB")

        self.t0astDBConn = Tier0DB.Tier0DB(self.t0astDBConfig,
                                           manageGlobal = manageGlobal)
        self.t0astDBConn.connect()

        return

    def connectPADB(self):
        """
        _connectPADB_

        Connect to the ProdAgent database.  

        Note that connection parameters are pulled from the "ProdAgentDB"
        section of the ProdAgent configuration file.
        """
        paConfig = loadProdAgentConfiguration()
        self.paDBConfig = paConfig.getConfig("ProdAgentDB")

        self.paDBConn = Tier0DB.Tier0DB(self.paDBConfig)
        self.paDBConn.connect()

        return    

    def lookupParameter(self, parameter, default):
        """
        _lookupParameter_

        Check to see if a parameter exists in the ProdAgent config.
        If it does, return its value, otherwise return the default
        value.

        Note that all found parameters are returned as strings, while
        the type of the default parameter is not changes, simply returned
        if the parameter is not found in the configuration file.
        """
        if parameter not in self.args.keys():
            logging.error("%s not set in PA config, using default: %s" %
                          (parameter, default))
            return default
        else:
            return self.args[parameter]

    def initRunConfigCache(self):
        """
        _initRunConfigCache_
    
        Setup the RunConfigCache.  If there is a "fileBackend" parameter in the
        Tier0DB section of the ProdAgent configuration file the RunConfigCache
        will be populated from a file, otherwise it will be populated from
        T0AST/ConfDB.
        """
        if self.runConfigCache == None:
            if self.t0astDBConn == None:
                logging.debug("initRunConfigCache(): calling connectT0AST()")
                self.connectT0AST()
    
            fileBackend = None
            
            if "fileBackend" in self.t0astDBConfig.keys():
                fileBackend = self.t0astDBConfig["fileBackend"]
                logging.debug("Using fileBackend %s for RunConfigCache" %
                              self.t0astDBConfig["fileBackend"])
            else:
                logging.debug("Using RunSummary/ConfDB for RunConfigCache")
                        
            self.runConfigCache = getRunConfigCache(self.t0astDBConn,
                                                    fileBackend)
        
    def getRunConfig(self, runNumber):
        """
        _getRunConfig_
    
        Get a RunConfig object from the RunConfigCache.  Will return None if
        there is an error retrieving the RunConfig for a particular run.
        """
        if self.runConfigCache == None:
            self.initRunConfigCache()
    
        return self.runConfigCache.getRunConfig(runNumber)

    def publishAlert(self, severity, message):
        """
        _publishAlert_

        Publish an alert to the alert system.
        """
        componentName = self.__class__.__name__.replace("Component", "")
        #self.alertSystem.publishAlert(severity, componentName, message)

        return

    def messageLoop(self):
        """
        _messageLoop_

        Loop that reads messages from the message passing system and dispatches
        them to the __call__() method of the subclass.  This will also catch
        any component crashes and post a message about it to the AlertHandler
        system.
        """
        reconnect = False

        # setup Heartbeat message
        heartbeatMsg = self.componentName + ":heartbeat"
        self.ms.subscribeTo( heartbeatMsg )
        self.ms.publishUnique( heartbeatMsg, "", self.heartbeat )

        # setup Shutdown messages
        shutdownMsg = self.componentName + ":shutdown"
        shutdownAllMsg = "All:shutdown"
        self.ms.remove( shutdownMsg )
        self.ms.remove( shutdownAllMsg )
        self.ms.subscribeTo( shutdownMsg )
        self.ms.subscribeTo( shutdownAllMsg )

        while True:
            try:
                if not reconnect:
                    msgtype, payload = self.ms.get()
                    self.ms.commit()
                    
                if msgtype == heartbeatMsg:
                    if self.t0astDBConn != None:
                        self.t0astDBConn.beginTransaction()
                        logging.debug("Updating %s heartbeat" % self.componentName)
                        InsertHeartBeat.updateHeartBeat(self.t0astDBConn, self.componentName)
                        self.t0astDBConn.commit()
                    self.ms.publishUnique( heartbeatMsg, "", self.heartbeat )
                elif msgtype == shutdownMsg \
                  or msgtype == shutdownAllMsg:
                    logging.info("Shutdown message received, going down")
                    if self.t0astDBConn:
                        self.t0astDBConn.commit()
                        self.t0astDBConn.close()
                    if self.paDBConn:
                        self.paDBConn.commit()
                        self.paDBConn.close()
                    return
                else:
                    self.__call__(msgtype, payload)

                reconnect = False
            except Exception, ex:
                if str(ex) == "0":
                    return
                if reconnect == True:
                    logging.debug("Couldn't reconnect, bailing...")                
                elif "orig" in dir(ex):
                    if type(ex.orig) != tuple and \
                           ex.orig == "ORA-25408: can not safely replay call":
                        logging.debug("Oracle connection died, restarting...")
                        reconnect = True
                        self.t0astDBConn = None
                        self.connectT0AST()
                        continue
                    elif ex.orig[0] == 2006 and ex.orig[1] == "MySQL server has gone away":
                        logging.debug("PA Connection died, restarting...")
                        reconnect = True
                        self.t0astDBConn.rollback()
                        self.connectPADB()
                        continue

                            
                crashMessage = "Component crashed with exception: " + str(ex)
                crashMessage += "\nStacktrace:\n"
                
                stackTrace = traceback.format_tb(sys.exc_info()[2], None)
                for stackFrame in stackTrace:
                    crashMessage += stackFrame
                    
                logging.error(crashMessage)
                self.publishAlert("Critical", crashMessage)
                raise Exception, ex

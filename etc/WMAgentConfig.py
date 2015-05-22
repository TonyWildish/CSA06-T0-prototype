#!/usr/bin/env python
"""
WMAgent Configuration

Configuration used for running the DBSUpload WMAgent component in the Tier1
Skimming system.
"""

__revision__ = "$Id: WMAgentConfig.py,v 1.1 2009/05/22 15:51:16 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Configuration import Configuration
config = Configuration()

config.section_("Agent")
config.Agent.hostName = "cmssrv18.fnal.gov"
config.Agent.contact = "sfoulkes@fnal.gov"
config.Agent.teamName = "DMWM"
config.Agent.agentName = "Tier1Skimmer"

config.section_("General")
config.General.workDir = "/home/sfoulkes/work"

config.section_("CoreDatabase")
config.CoreDatabase.dialect = "mysql"
config.CoreDatabase.socket = "/opt/MySQL.5.0/var/lib/mysql/mysql.sock"
config.CoreDatabase.hostname = "localhost"
config.CoreDatabase.masterUser = "sfoulkes"
config.CoreDatabase.masterPasswd = ""
config.CoreDatabase.user = "sfoulkes"
config.CoreDatabase.passwd = ""
config.CoreDatabase.name = "ProdAgentDB_sfoulkes"

config.component_("DBSUpload")
config.DBSUpload.namespace = "WMComponent.DBSUpload.DBSUpload"
config.DBSUpload.ComponentDir = config.General.workDir + "/Components/DBSUpload"
config.DBSUpload.logLevel = "DEBUG"
config.DBSUpload.maxThreads = 1
config.DBSUpload.bufferSuccessHandler = "WMComponent.DBSUpload.Handler.BufferSuccess"
config.DBSUpload.newWorkflowHandler = "WMComponent.DBSUpload.Handler.NewWorkflowHandler"
config.DBSUpload.dbsurl = "http://cmssrv49.fnal.gov:8989/DBS/servlet/DBSServlet"
config.DBSUpload.dbsversion = "DBS_2_0_6"
config.DBSUpload.uploadFileMax = 10

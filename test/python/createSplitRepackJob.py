
import sys
import os

from T0.RepackMgr.RepackerGenerator import RepackerGenerator
from T0.SplitInjector.RepackerSplitGenerator import SplitJobGenerator
from ProdCommon.Database import DbSession

params= \
 {
  'WorkDir':os.getcwd(),
  #"LumiServerUrl" : "http://cmsmon.cern.ch/lumi/servlet/LumiServlet",
  #"LumiServerUrl" : "http://cmssrv18.fnal.gov:8080/lumi/servlet/LumiServlet",
  "LumiServerUrl" : "http://lxb5565.cern.ch:8180/lumi/servlet/LumiServlet",
  'CMSSW_version':"CMSSW_1_6_0_pre9",
  'CMSSW_path':'/uscmst1/prod/sw/cms/',
  'CMSSW_arch':"slc4_ia32_gcc345"
 }
dbcfg={'dbName':'CMSCALD',
         'host':'cmscald',
         'user':'REPACK_DEV',
       'passwd':'***',
'socketFileLocation':'',
       'portNr':'',
'refreshPeriod' : 4*3600 ,
'maxConnectionAttempts' : 5,
'dbWaitingTime' : 10,
      'dbType' : 'oracle',
      }

db=DbSession.getSession(dbcfg)
repack=RepackerGenerator(int(sys.argv[1]),"L1",params,db)
split_plugin=SplitJobGenerator()
repack.registerAlgoPlugin('split',split_plugin)
report=repack.pollAndCreateJobs()
if(not report):
    print "No new files/trigger sections"
    sys.exit(0)
print report
for algo in report.keys():
    fname,updated_ts,job_name=report[algo]
    repack.updateTsStatus(updated_ts.keys(),job_name)


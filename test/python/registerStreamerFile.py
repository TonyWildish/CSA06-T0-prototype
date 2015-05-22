
import xml.sax
import xml.sax.handler
import sys
from ProdCommon.Database import DbSession
from T0.State.Database.Writer import InsertRun
from T0.State.Database.Writer import InsertLumi
from T0.State.Database.Writer import InsertTrigger
from T0.State.Database.Writer import InsertStreamer


class StreamerIndex(xml.sax.handler.ContentHandler):
    def __init__(self,index_fn):
        self.index_fn=index_fn
        self.d_trigger_path={}
        f=open(self.index_fn,"r")
        xml.sax.parse(f,self)
        f.close()


    def startElement(self,name,attrs):
        #print name,attrs.getNames()
        if(name=="StreamerIndex"):
            self.run=int(attrs.getValue("Run"))
            self.lumi=int(attrs.getValue("Lumi"))
            self.total_events=int(attrs.getValue("TotalEvents"))
            return
        if(name=="TriggerPath"):
            tn=attrs.getValue("Name")
            self.curr_trigger_path=tn.encode('Latin-1')
            self.d_trigger_path[self.curr_trigger_path]={}
            return
        if(name=="EventCount"):
            v=attrs.getValue("Value")
            self.d_trigger_path[self.curr_trigger_path]["event_count"]=int(v)
            return
        if(name=="ErrorCount"):
            v=attrs.getValue("Value")
            self.d_trigger_path[self.curr_trigger_path]["error_count"]=int(v)
            return

            
    def endElement(self,name):
        if(name=="TriggerPath"):
            self.curr_trigger_path=None
            return


def registerStreamerFile(db,lfn,index_file, status):
    si=StreamerIndex(index_file)
    InsertStreamer.addNewStreamerDBS(db, si.run, si.lumi, lfn, 0, 0, si.total_events, status, si.d_trigger_path)


if(__name__=="__main__"):
    if(len(sys.argv)<3):
        print "Usage: %s lfn index_file [generate_test_data]"%sys.argv[0]
        sys.exit(1)
    lfn=sys.argv[1]
    sfn=sys.argv[2]
    is_test=0
    if(len(sys.argv)>=4 and sys.argv[3]=='generate_test_data'):
        is_test=1
    dbcfg={'dbName':'CMSCALD',
           'host':'cmscald',
           'user':'REPACK_DEV',
           'passwd':'***',
           'socketFileLocation':'',
           'portNr':'',
           'refreshPeriod' : 4*3600 ,
           'maxConnectionAttempts' : 5,
           'dbWaitingTime' : 10,
           'dbType' : 'oracle'}
    db=DbSession.getSession(dbcfg)
    print"Using index file",sfn
    db.begin()
    if(is_test):
        si=StreamerIndex(sfn)
        print si.run,si.lumi,si.total_events
        print si.d_trigger_path
        print "Registering run",si.run
        InsertRun.insertRunDBS(db,si.run,0,0,"OK",True)
        InsertLumi.insertLumiDBS(db,si.run,si.lumi,0,True)
        for i in si.d_trigger_path.keys():
            InsertTrigger.insertTriggerDBS(db,si.run,i,"pri_"+i,"split",True)
        InsertStreamer.deleteStreamerDBS(db,lfn)
    registerStreamerFile(db,lfn,sfn, "NEW")
    db.commit()

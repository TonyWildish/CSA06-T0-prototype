#!/usr/bin/env python


import unittest
import time
import os
import hotshot, hotshot.stats
import T0.State.Database.Writer.InsertStreamer as InsertStreamer
from T0.State.Database.Writer.WriteError import WriteError
from T0.State.Database.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session

import TestUtils as Utils

class InsertStreamerTest(unittest.TestCase):
    """
    TestCase for InsertStreamer module
    """
    def setUp(self):
        self.profiles = set()
        

    def tearDown(self):
        for prof in self.profiles:
            if os.path.exists(prof):
                os.remove(prof) 
        self.profiles = set()     


    def testA(self):
        """basic tests"""

        run = Utils.create1Run()
        lumis = Utils.createLumis(run)
        triggers = Utils.createTriggers()

        try:

	    lfn = "Run%s-Lumi%s-file.dat" % (run, lumis[0])
            splitTrigs = {}
            [ splitTrigs.__setitem__(x, 1000) for x in  triggers.keys()[0:25]]
            accumTrigs = {}
            [ accumTrigs.__setitem__(x, 1000) for x in triggers.keys()[25:]]

	    sid = InsertStreamer.addNewStreamer(run, lumis[0],  lfn, 12345678, 50000, splitTrigs, accumTrigs)
	    Session.commit_all()	

	    """

            lfn = "Run%s-Lumi%s-file.dat" % (run, lumis[0])
                                             
            sid = InsertStreamer.newStreamer(run, lumis[0],  lfn, 
						pfn="/path/to/%s" % lfn, 
						filesize=12345678, events=50000)
            Session.commit_all()

            splitTrigs = {}
            [ splitTrigs.__setitem__(x, 1000) for x in  triggers.keys()[0:25]]
            accumTrigs = {}
            [ accumTrigs.__setitem__(x, 1000) for x in triggers.keys()[25:]]
            
            InsertStreamer.addTriggerTags(sid, run, lumis[0],
                                          splitTrigs,
                                          accumTrigs)
            Session.commit_all()
            """


        finally:
            # always clean up
            Utils.delete1Run()
            Utils.deleteTriggers()
            pass


    def testB(self):
        """extended profiled tests"""
        
        run = Utils.create1Run()
        lumis = Utils.createLumis(run)
        triggers = Utils.createTriggers()
        try:
            insertProf = hotshot.Profile("InsertStreamer_testB.prof")
            self.profiles.add("InsertStreamer_testB.prof")
            insertProf.start()
            for lumi in lumis:
            
                for filenum in range(0, 10):

		    lfn = "Run%s-Lumi%s-file%s.dat" % (run, lumi, filenum)
		    splitTrigs = {}
                    [ splitTrigs.__setitem__(x, 1000) for x in  triggers.keys()[0:25]]
                    accumTrigs = {}
                    [ accumTrigs.__setitem__(x, 1000) for x in triggers.keys()[25:]]

		    sid = InsertStreamer.addNewStreamer(run, lumi,  lfn,
							12345678, 50000, splitTrigs, accumTrigs)
			
                    Session.commit_all()
            insertProf.stop()
            Utils.printProfileSummary("InsertStreamer_testB.prof")
        finally:
            # always clean up
            Utils.delete1Run()
            Utils.deleteTriggers()
            pass
        
        
if __name__ == '__main__':
    Session.set_database(dbConfig)
    Session.connect()
    Session.start_transaction()
    
    unittest.main()
    Session.commit_all()
    Session.close_all()

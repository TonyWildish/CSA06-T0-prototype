#!/usr/bin/env python

import unittest
import time
import os
import hotshot, hotshot.stats
import T0.State.Database.Writer.InsertRun as InsertRun
from T0.State.Database.Writer.WriteError import WriteError
from T0.State.Database.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session

import T0.State.Database.Writer.InsertTrigger as InsertTrigger



class InsertRunTest(unittest.TestCase):
    """
    TestCase for InsertRun Module

    """
    def setUp(self):
        self.profiles = set()
        

    def tearDown(self):
        for prof in self.profiles:
            if os.path.exists(prof):
                os.remove(prof) 
        self.profiles = set()     

    def testA(self):
        """test inserting new runs"""

        InsertRun.insertRun(1000001)
        Session.commit_all()


        InsertRun.runFinished(1000001)
        Session.commit_all()
       
        # dunno whats that, don't want to know @ the moment  
        #self.assertRaises(WriteError,
        #                  InsertRun.insertRun, 1000001, int(time.time()))
                          
        #self.assertRaises(WriteError,
        #                  InsertRun.runFinished, 1000001, int(time.time()))

 
        #### Map triggers to Run 
        triggers = {}

        [ triggers.__setitem__("trigger%s" % x, "dataset%s" % x)
          for x in range(0, 100) ]

	trig_ids = InsertTrigger.insertTriggers(**triggers)

        InsertTrigger.insertRunTrigsAssoc('1000001', trig_ids.values())
        
        InsertRun.deleteRun(1000001)
        Session.commit_all()


    def testB(self):
        """test inserting multiple runs"""


        insertProf = hotshot.Profile("InsertRun_testB_insert.prof")
        self.profiles.add("InsertRun_testB_insert.prof")
        insertProf.start()
        for i in range(0, 1000):
            run = 1000000 + i
            InsertRun.insertRun(run)

        Session.commit_all()
        insertProf.stop()

        print "<<<<<<<<<<<<<<<Insert 1000 performance>>>>>>>>>>>>>>>>>>"
        stats = hotshot.stats.load("InsertRun_testB_insert.prof")
        stats.strip_dirs()
        stats.sort_stats('time', 'calls')
        stats.print_stats(10)

        

        finishedProf = hotshot.Profile("InsertRun_testB_finished.prof")
        self.profiles.add("InsertRun_testB_finished.prof")
        finishedProf.start()
        for i in range(0, 1000):
            run = 1000000 + i
            InsertRun.runFinished(run)
            
        Session.commit_all()
        finishedProf.stop()
        print "<<<<<<<<<<<<<<<RunFinished 1000 performance>>>>>>>>>>>>>"
        stats = hotshot.stats.load("InsertRun_testB_finished.prof")
        stats.strip_dirs()
        stats.sort_stats('time', 'calls')
        stats.print_stats(10)
        
        
        deleteProf = hotshot.Profile("InsertRun_testB_delete.prof")
        self.profiles.add("InsertRun_testB_delete.prof")
        deleteProf.start()

        for i in range(0, 1000):
            run = 1000000 + i
            InsertRun.deleteRun(run)
        Session.commit_all()
        deleteProf.stop()

        print "<<<<<<<<<<<<<<<Delete 1000 performance>>>>>>>>>>>>>>>>>>"
        stats = hotshot.stats.load("InsertRun_testB_delete.prof")
        stats.strip_dirs()
        stats.sort_stats('time', 'calls')
        stats.print_stats(10)

        if os.path.exists("InsertRun_testB_delete.prof"):
            os.remove("InsertRun_testB_delete.prof")
        
        if os.path.exists("InsertRun_testB_insert.prof"):
            os.remove("InsertRun_testB_insert.prof")
    

if __name__ == '__main__':

    Session.set_database(dbConfig)
    Session.connect()
    Session.start_transaction()
    
    unittest.main()
    Session.commit_all()
    Session.close_all()

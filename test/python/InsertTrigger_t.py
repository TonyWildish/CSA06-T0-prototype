#!/usr/bin/env python

import unittest
import time
import os
import hotshot, hotshot.stats
import T0.State.Database.Writer.InsertTrigger as InsertTrigger
from T0.State.Database.Writer.WriteError import WriteError
from T0.State.Database.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session


import TestUtils as Utils

class InsertTriggerTest(unittest.TestCase):
    """
    TestCase for InsertTrigger module
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

        InsertTrigger.insertTriggers( trigger1 = ("dataset1", "split") )
        Session.commit_all()

        #self.assertRaises(WriteError,
        #                  InsertTrigger.insertTriggers, trigger1 = "dataset1")

        InsertTrigger.deleteTriggers( "trigger1")

        Session.commit_all()


    def testB(self):
        """extended test with profile"""

        triggers = {}

        [ triggers.__setitem__("trigger%s" % x, ("dataset%s" % x, "split") )
          for x in range(0, 100) ]

        insertProf = hotshot.Profile("InsertTrigger_testB_insert.prof")
        self.profiles.add("InsertTrigger_testB_insert.prof")
        insertProf.start()

        ids = InsertTrigger.insertTriggers(**triggers)

        Session.commit_all()
        insertProf.stop()
        Utils.printProfileSummary("InsertTrigger_testB_insert.prof")

        deleteProf = hotshot.Profile("InsertTrigger_testB_delete.prof")
        self.profiles.add("InsertTrigger_testB_delete.prof")
        deleteProf.start()

        InsertTrigger.deleteTriggers(*triggers.keys())
        Session.commit_all()
        deleteProf.stop()
        Utils.printProfileSummary("InsertTrigger_testB_delete.prof")
        
        
if __name__ == '__main__':
    Session.set_database(dbConfig)
    Session.connect()
    Session.start_transaction()
    
    unittest.main()
    Session.commit_all()
    Session.close_all()

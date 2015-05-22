#!/usr/bin/env python

import unittest
import time
import os
import hotshot, hotshot.stats
import T0.State.Database.Writer.InsertLumi as InsertLumi
from T0.State.Database.Writer.WriteError import WriteError
from T0.State.Database.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session


import TestUtils as Utils

class InsertLumiTest(unittest.TestCase):
    """
    TestCase for InsertLumi module
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


        import pdb
        pdb.set_trace()

        run = Utils.create1Run()
        try:
            InsertLumi.insertLumi(run, 1)
            Session.commit_all()

            InsertLumi.insertLumi(run, 2)
            Session.commit_all()
            InsertLumi.insertLumi(run, 3)
            Session.commit_all()

            #self.assertRaises(WriteError, InsertLumi.insertLumi, run, 1,
            #                  int(time.time()))

        finally:
            # always clean up
            Utils.delete1Run()
        

    def testB(self):
        """large scale test with profiling"""
        runs = Utils.create1000Runs()

        try:

            insertProf = hotshot.Profile("InsertLumi_testB_insert.prof")
            self.profiles.add("InsertLumi_testB_insert.prof")
            insertProf.start()
            
            for run in runs:
                for i in range(0,6):
                    
                    InsertLumi.insertLumi(run, i)
            Session.commit_all()
            insertProf.stop()
            Utils.printProfileSummary("InsertLumi_testB_insert.prof")            

        finally:
            # always clean up
            Utils.delete1000Runs()
        


if __name__ == '__main__':

    import pdb
    pdb.set_trace()

    Session.set_database(dbConfig)
    Session.connect()
    Session.start_transaction()
    
    unittest.main()
    Session.commit_all()
    Session.close_all()

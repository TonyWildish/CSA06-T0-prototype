from ProdCommon.Database import Session

import T0.State.Database.Reader.ListStreamerFiles as ListStreamerFiles
from T0.State.Database.Config import defaultConfig as dbConfig

def listStreamerTest01(runNumber):

	ListStreamerFiles.listStreamerFiles(runNumber)


if __name__ == '__main__':

    Session.set_database(dbConfig)
    Session.connect()
    Session.start_transaction()

    listStreamerTest01(1234)

    #Session.commit_all()
    Session.close_all()





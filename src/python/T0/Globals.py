"""
save all the global variables used by Tier0
Currently just WMBS subscription

"""
import traceback
import sys
import logging
 
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription

from ProdAgentCore.Configuration import loadProdAgentConfiguration

class GlobalValues:
    """
    Namespace like Container 

    """
    _expressMergeSubscription = None
    _alcaSubscription = None
    _alcaDefSubName = "AlcaSkimmable"
    _alcaSubName = _alcaDefSubName
    _alcaNewSelection = None
    _combinedAlcaSubscription = None
    _expressDBSSubscription = None
    _expressTransferSubscription = None
    _dbsUploadSubscription = None
    _transferSubscription = None
    _recoSubscription = None
    _deleteSubscription = None
    TIER_ORDER = [ "RAW", "RECO", "ALCARECO"]
    CAF_NODE = "T2_CH_CAF"
    
def _getSubscription(workflow, fileset):
    workflow.load()
    fileset.load()
    subscription = Subscription(fileset = fileset, workflow = workflow)
    subscription.load()
    return subscription
    
def alcaNewSelection():
    if GlobalValues._alcaNewSelection == None:
        GlobalValues._alcaNewSelection = False
        paConfig = loadProdAgentConfiguration()
        runConfig = paConfig.getConfig( "RunConfig" )
        parameter = "AlcaSubscription"
        if parameter in runConfig:
            GlobalValues._alcaSubName = runConfig[parameter]
            if GlobalValues._alcaDefSubName != GlobalValues._alcaSubName:
                GlobalValues._alcaNewSelection = True
    return GlobalValues._alcaNewSelection

def retrieveAlcaSubscription():
    """
    _retrieveAlcaSubscription_
    
    retrieve wmbs Subscription from the cache or database
    Core information should be constant (i.e. name, id) but might be needed to 
    loadData from caller for further processing 
    """
    if GlobalValues._alcaSubscription == None:
        # This method will set subscription name from config
        alcaNewSelection()
        GlobalValues._alcaSubscription = \
           _getSubscription(Workflow(spec = "FileAlcaSkim", 
                                  owner = "CMSTier0",
                                  name = "FileAlcaSkim"),
                            Fileset( name = GlobalValues._alcaSubName )
                           ) 
        
    return GlobalValues._alcaSubscription

def retrieveExpressDBSSubscription():
    """
    _retrieveExpressDBSSubscription_
    
    retrieve wmbs Subscription from the cache or database
    Core information should be constant (i.e. name, id) but might be needed to 
    loadData from caller for further processing 
    """
    if GlobalValues._expressDBSSubscription == None:
        GlobalValues._expressDBSSubscription = \
            _getSubscription(Workflow(spec = "ExpressDBSUpload", 
                                      owner = "CMSTier0",
                                      name = "ExpressDBSUpload"),
                             Fileset(name = "ExpressDBSUploadable")
                             )
             
    return GlobalValues._expressDBSSubscription

def retrieveExpressTransferSubscription():
    """
    _retrieveExpressTransferSubscription_
    
    retrieve wmbs Subscription from the cache or database
    Core information should be constant (i.e. name, id) but might be needed to 
    loadData from caller for further processing 
    """
    if GlobalValues._expressTransferSubscription == None:
        GlobalValues._expressTransferSubscription = \
            _getSubscription(Workflow(spec = "ExpressTransfer", 
                                      owner = "CMSTier0",
                                      name = "ExpressTransfer"),
                             Fileset(name = "ExpressTransferable")
                             )

    return GlobalValues._expressTransferSubscription

def retrieveDBSUploadSubscription():
    """
    _retrieveDBSUploadSubscription_
    
    retrieve wmbs Subscription from the cache or database
    Core information should be constant (i.e. name, id) but might be needed to 
    loadData from caller for further processing 
    """
    if GlobalValues._dbsUploadSubscription == None:
        GlobalValues._dbsUploadSubscription = \
            _getSubscription(Workflow(spec = "FileDBSUpload", 
                                      owner = "CMSTier0",
                                      name = "FileDBSUpload"),
                             Fileset(name = "DBSUploadable")
                             )
             
    return GlobalValues._dbsUploadSubscription

def retrieveTransferSubscription():
    """
    _retrieveTransferSubscription_
    
    retrieve wmbs Subscription from the cache or database
    Core information should be constant (i.e. name, id) but might be needed to 
    loadData from caller for further processing 
    """
    if GlobalValues._transferSubscription == None:
        GlobalValues._transferSubscription = \
            _getSubscription(Workflow(spec = "FileTransfer", 
                                      owner = "CMSTier0",
                                      name = "FileTransfer"),
                             Fileset(name = "Transferable")
                             )
            
    return GlobalValues._transferSubscription

def retrieveRecoSubscription():
    """
    _retrieveRecoSubscription_
    
    retrieve wmbs Subscription from the cache or database
    Core information should be constant (i.e. name, id) but might be needed to 
    loadData from caller for further processing 
    """
    if GlobalValues._recoSubscription == None:
        GlobalValues._recoSubscription = \
            _getSubscription(Workflow(spec = "FileReconstruction", 
                                      owner = "CMSTier0",
                                      name = "FileReconstruction"),
                             Fileset(name = "Reconstructable")
                             )
            
    return GlobalValues._recoSubscription
 
def retrieveDeleteSubscription():
    """
    _retrieveDeleteSubscription_
    
    retrieve wmbs Subscription from the cache or database
    Core information should be constant (i.e. name, id) but might be needed to 
    loadData from caller for further processing 
    """
    if GlobalValues._recoSubscription == None:
        GlobalValues._deleteSubscription =  \
            _getSubscription(Workflow(spec = "FileDelete", 
                                      owner = "CMSTier0",
                                      name = "FileDelete"),
                             Fileset(name = "Deletable")
                             )
            
    return GlobalValues._deleteSubscription

def stackTraceLog(errorMessage):
    crashMessage = ""
    stackTrace = traceback.format_tb(sys.exc_info()[2], None)
    for stackFrame in stackTrace:
        crashMessage += stackFrame
    logging.error("""Error, Stack trace: \n\t%s""" % str(errorMessage))        
    logging.error(crashMessage)
    
    return crashMessage

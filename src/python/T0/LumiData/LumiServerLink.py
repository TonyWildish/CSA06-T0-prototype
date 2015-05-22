"""
_LumiServerLink_

Provides LumiData from LumiServer and populates JobSpec's config
file with lumi data.

"""

__version__ = "$Revision: 1.6 $"
__revision__ = \
      "$Id: LumiServerLink.py,v 1.6 2008/07/24 20:47:34 lueking Exp $"
__author__ = "evansde"


import logging

from LumiWebClient.lumiException import LumiException
from LumiWebClient.lumiApi import LumiApi

#from T0.LumiData import LumiProducer


class LumiServerError(Exception):
    """
    _LumiServerError_

    Error class for LumiServer interactions

    """
    def __init__(self, msg):
        Exception.__init__(self, msg)
        

class LumiServerLink:
    """
    _LumiServerLink_

    API to retrieve Lumi information from the Lumi Server and generate
    a config file fragment that can be added to a process object

    """
    def __init__(self, serverUrl):
        self.connection = None
        self.summaryCache = {}
        self.detailCache = {}
        self.lumiserver = serverUrl
        


    def connect(self):
        """
        _connect_

        Connect to the lumi server

        """
        try:
            self.connection = LumiApi({"url" :self.lumiserver})
        except LumiException, ex:
            msg = "Error in LumiServerLink with LumiApi\n"
            msg += "Caught Exception: %s: %s "  % (ex.msg, ex.code )
            msg += "%s\n" % ex
            raise LumiServerError(msg)
        
        return



    


    def __call__(self, run, *lumilist):
        """
        _operator(run, lumi)_

        Retrieve the lumi information for all lumi sections

        """
        self.connect()
        lumiData = {}
        for lumiSection in lumilist:
            hashedVal = "%s.%s" % (run, lumiSection)
            lumiData[hashedVal] = self.getLumiInfo(run, lumiSection)

        return lumiData
    
    def getLumiInfo(self, runNumber, lumiSectionNumber):
        """
        _getLumiInfo_

        retrieve the lumi information for the run/lumi pair provided
        from the server.

        Add it to the cache in this object
        
        """
        lumiInfo = {"lsnumber" : lumiSectionNumber}
       
        if(not self.connection):
            return lumiInfo

        logging.info("Getting LumiInfo for run %s lumisection %s" % (
            runNumber,lumiSectionNumber))
        
        lumiSum = self.getSummary(runNumber, lumiSectionNumber)
        print "LumiSummary1:", lumiSum
        if(not lumiSum):
            return lumiInfo
        #import pdb
	#pdb.set_trace()

	# YG: It should not take ET, instead it should be general. Put check
        # if not desired found, raise exception
        if('instant_lumi' not in lumiSum.keys() or 'instant_lumi_err' not in lumiSum.keys()
            or 'instant_lumi_qlty' not in lumiSum.keys() 
            or 'deadtime_normalization'  not in lumiSum.keys()):
            msg = "No Completed lumi summary data found in DB for Run=%s Lumi=%s \n" % (runNumber, lumiSectionNumber)
            raise LumiServerError(msg)
        lumiInfo['avginslumi'] = float(lumiSum['instant_lumi'])
        lumiInfo['avginslumierr'] = float(lumiSum['instant_lumi_err'])
        # debug
        #print "lumiSum['instant_lumi_qlty'] =", `lumiSum['instant_lumi_qlty']`
        #print "long(float(lumiSum['instant_lumi_qlty']) =", `long(float(lumiSum['instant_lumi_qlty']))`
        lumiInfo['lumisecqual'] = long(float(lumiSum['instant_lumi_qlty']))
        #print  "lumiInfo['lumisecqual'] =", `lumiInfo['lumisecqual']`
        lumiInfo['deadfrac'] = float(lumiSum['deadtime_normalization'])
        
        #lumiInfo['avginslumi'] = float(lumiSum.get('instant_et_lumi', '0.0'))
        #lumiInfo['avginslumierr'] = float(lumiSum.get('instant_et_lumi_err', '0.0'))
        #lumiInfo['deadfrac'] = float(lumiSum.get('live_frac', '0.0'))
        #lumiInfo['lumisecqual'] = long(float(lumiSum.get('instant_et_lumi_qlty', '0')))
        lumiInfo['det_et_sum'] = []
        lumiInfo['det_et_err'] = []
        lumiInfo['det_et_qua'] = []
        lumiInfo['det_occ_sum'] = []
        lumiInfo['det_occ_err'] = []
        lumiInfo['det_occ_qua'] = []

        lumiEtDet = self.getDetails(runNumber, lumiSectionNumber, "ET")
        print "Lumi Details ET:", lumiEtDet
        oldBunch = 0
        for b in lumiEtDet:
            #YG: check for the key. If not found, return empty ones for all ET.
            if('bunch_number' not in b.keys() or 'ET'not in b.keys()):
                break 
            bunch = int(b['bunch_number'])
            # allow to start with bunch 0 or 1
            if ( bunch == 0 ):
                old_bunch = -1
            if(bunch - oldBunch != 1):
                msg = "Error in ET bunch sequence: %d %d" % (oldBunch, bunch)
                #YG: That means all detailed data (ET & OCC) are empty. 
                raise LumiServerError(msg)
            oldBunch = bunch
            summed = float(b['ET']['et_lumi'])
            err = float(b['ET']['et_lumi_err'])
            qua = int(float(b['ET']['et_lumi_qlty']))
            lumiInfo['det_et_sum'].append(summed)
            lumiInfo['det_et_err'].append(err)
            lumiInfo['det_et_qua'].append(qua)
            
        lumiOccDet = self.getDetails(runNumber, lumiSectionNumber, "OCC")
        print "LumiDetails OCC:",lumiOccDet

        oldBunch = 0
        for b in lumiOccDet:
            #  //To Follow up:
            # // Is this valid? Or is this an exception if there is no OCC
            #//  data in the field??
            #if not b.has_key("OCC"):
            #    continue

            #YG: check for the key. If not found, return empty ones for all OCC.
            if('bunch_number' not in b.keys() or 'OCC'not in b.keys()):
                break
            bunch = int(b['bunch_number'])
            # allow to start with bunch 0 or 1
            if ( bunch == 0 ):
                old_bunch = -1
            if(bunch - oldBunch != 1):
                #YG: need to check with Valerie.
                msg = "Error in OCC bunch sequence: %d %d" % (oldBunch, bunch)
                raise LumiServerError(msg)
            oldBunch = bunch
            summed = float(b['OCC']['occ_lumi'])
            err =  float(b['OCC']['occ_lumi_err'])
            qua = int(float(b['OCC']['occ_lumi_qlty']))
            lumiInfo['det_occ_sum'].append(summed)
            lumiInfo['det_occ_err'].append(err)
            lumiInfo['det_occ_qua'].append(qua)

        return lumiInfo

    def getSummary(self, run, lumisection):
        """
        _getSummary_

        Get the SUMMARY information for a given run/lumi from the server

        Summary information is cached in this object and reused if
        possible
        
        """
        logging.info("Getting LumiInfo SUMMARY for run %s lumisection %s" %
                     (str(run), str(lumisection) )
                     )

        cachekey = "%s.%s" % (str(run), str(lumisection))
        summary = self.summaryCache.get(cachekey, None)
        if  summary != None:
            return summary
        try:
            summaryList = self.connection.listLumiSummary(
                str(run), str(lumisection))
        except LumiException, ex:
            msg = "LumiException:Failed to retrieve lumi summary data for "
            msg += "Run=%s Lumi=%s\n" % (run, lumisection)
            msg += "Caught Exception: %s: %s "  % (ex.msg, ex.code )
            raise LumiServerError(msg)
        except Exception, ex:
            msg = "Error retrieving Lumi Summary for run %s" % run
            msg += " lumi %s\n" % lumisection
            msg += str(ex)
            raise LumiServerError(msg)
        #check if lumi border server return an empty list
        if len(summaryList) == 0:
            msg = "Unknown Run/Lumi summary for "
            msg += "Run=%s Lumi=%s\n" % (run, lumisection)
            raise LumiServerError(msg)
        
        self.summaryCache[cachekey] = summaryList[-1]
        return self.summaryCache[cachekey]
        
    

    def getDetails(self, run, lumisection, lumiOption):
        """
        _getDetails_

        Get detailed information for a run/lumisection

        

        """
        logging.info("Getting Lumi DETAILS %s for run %s lumisection %s" % (
            lumiOption, str(run), str(lumisection)))
        cachekey = "%s.%s.%s" % (lumiOption,
                                 str(run),
                                 str(lumisection))
        details = self.detailCache.get(cachekey, None)
        if details != None:
            return details

        try:    
            details = self.connection.listLumiByBunch(
                str(run), str(lumisection), lumiOption)
        except LumiException, ex:
            msg = "LumiException retrieving Lumi Details for run %s" % run
            msg += " lumi %s, option=%s\n" % (lumisection, lumiOption)
            msg += "Caught LUMIServer Exception: %s: %s "  % (ex.msg, ex.code )
            raise LumiServerError(msg)
        except Exception, ex:
            msg = "Error retrieving Lumi Details for run %s" % run
            msg += " lumi %s, option=%s\n" % (lumisection, lumiOption)
            msg += str(ex)
            raise LumiServerError(msg)

        self.detailCache[cachekey] = details
        return self.detailCache[cachekey]


if __name__ == '__main__':
    
    LUMI1 = LumiServerLink(
        "http://cmsmon.cern.ch/CMSLUMI/servlet/LumiServlet")
    print LUMI1(12000, 1, 2, 3, 4, 5).keys()
                   

    

##    def _int_setLumiData(self,cfgInstance,lumi_data):
##        if(len(lumi_data)< = 1):
##            logging.info("Insufficient lumi data - ignoring")
##            return
##        # Set lumi data here
##        #print "Set LumiData",lumi_data
##        #cfgInstance = pickle.loads(job_spec.payload.cfgInterface.rawCfg)
##        #print "PRODUCERS:",cfgInstance.producers_()
##        # Get producers list (lumi module is EDProducer)
##        producers_list=cfgInstance.producers_()
##        if(not producers_list.has_key('lumiProducer')):
##            LumiProducer.makeLumiProducer(cfgInstance)
##            producers_list=cfgInstance.producers_()
##        mod_lumi=producers_list['lumiProducer']
##        #print "LumiModule",mod_lumi.parameterNames_(),dir(mod_lumi)
##        #Get template pset for the lumi module
##        LumiProducer.makeLumiSection(mod_lumi,lumi_data)
##        #pset_name=mod_lumi.parameterNames_()[0]
##        #pset=getattr(mod_lumi,pset_name)

##        #Clean the template pset name
##        #delattr(mod_lumi,pset_name)
##        #print "LumiModule2",mod_lumi.parameterNames_()

##        #Create the real PSet name"
##        #pset_name="LS"+str(lumi_data['lsnumber'])
##        #pset.setLabel(pset_name)
##        #Set parameters
##        #pset.avginslumi=lumi_data['avginslumi']
##        #pset.avginslumierr=lumi_data['avginslumierr']
##        #pset.lumisecqual=int(lumi_data['lumisecqual'])
##        #pset.deadfrac=lumi_data['deadfrac']
##        #pset.lsnumber=int(lumi_data['lsnumber'])

##        #pset.lumietsum=lumi_data['det_et_sum']
##        #pset.lumietsumerr=lumi_data['det_et_err']
##        #pset.lumietsumqual=lumi_data['det_et_qua']
##        #pset.lumiocc=lumi_data['det_occ_sum']
##        #pset.lumioccerr=lumi_data['det_occ_err']
##        #pset.lumioccqual=lumi_data['det_occ_qua']
        
##        #Insert the pset into the lumi module
##        #setattr(mod_lumi,pset_name,pset)
        
##        # bla-bla
##        print "DUMP:",cfgInstance.dumpConfig()

##        # save spec after update
##        #job_spec.save(job_spec_file)
##        return



#lumi_server_dict={}
#
#def getLumiServerLink(args):
#    url=args['LumiServerUrl']
#    if(lumi_server_dict.has_key(url)):
#        return lumi_server_dict[url]
#    lsl=LumiServerLink(url=args["LumiServerUrl"],level=args["DbsLevel"])
#    lumi_server_dict[url]=lsl
#    return lsl

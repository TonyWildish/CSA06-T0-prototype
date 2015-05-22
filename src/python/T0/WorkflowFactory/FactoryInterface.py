#!/usr/bin/env python
"""
_FactoryInterface_


Basic interface class for all workflow factories.
Start out very lightweight API only definition and
evolve to contain more common helper utils

Basic principle is that this factory is instantiated
with a config file and release and is then
used to cut out per run, per dataset workflow specs and
provide an API to customise them as needed for tier0 ops.


"""

import os
import imp
import time
import logging

from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWAPILoader import CMSSWAPILoader
from ProdCommon.CMSConfigTools.ConfigAPI.CMSSWConfig import CMSSWConfig


def installEventContent(process, dataTier, outputModule):
    outputModule.outputCommands = getattr(process,dataTier+"EventContent").outputCommands 


class FactoryInterface:
    """
    _FactoryInterface_

    Base class for Workflow Factory APIs


    """
    def __init__(self, version, cmsPath, scramArch):
        self.cmssw = {}
        self.cmssw['CMSSWVersion'] = version
        self.cmssw['CMSPath'] = cmsPath
        self.cmssw['ScramArch'] = scramArch
        self.cmssw['version1'] = 0
        self.cmssw['version2'] = 0
        self.cmssw['version3'] = 0
        self.process = None

        cmsswVersionList = version.split("_")
        try:
            self.cmssw['version1'] = int(cmsswVersionList[1])
            self.cmssw['version2'] = int(cmsswVersionList[2])
            self.cmssw['version3'] = int(cmsswVersionList[3])
        except ValueError:
            logging.info("Warning: non standard CMSSW version: %s" % version)


    def createConfiguration(self, sourceType, **params):
        """
        _createConfiguration_

        Builds a configuration, does some release specifc
        customizations, adds a source and adds output modules

        Three options to make the configuration, based on a
        config file, getting it from the framework or
        making up an empty one from scratch

        """
        loader = CMSSWAPILoader(self.cmssw['ScramArch'],
                                self.cmssw['CMSSWVersion'],
                                self.cmssw['CMSPath'])

        try:
            loader.load()
        except Exception, ex:
            logging.error("Couldn't load CMSSW libraries: %s" % ex)
            return None

        import FWCore.ParameterSet.Config as cms

        # building process
        # either from config file, from the framework(release) or from scratch
        if params.has_key('configFile'):

            if params.has_key('outputModuleDetails'):

                self.process = self.createProcessFromFile(params['configFile'],
                                                          stripOutputModules = True)

            elif params.has_key('outputModuleTemplate'):

                self.process = self.createProcessFromFile(params['configFile'],
                                                          stripOutputModules = False)

                moduleTemplate = params['outputModuleTemplate']

                # override primary dataset
                if moduleTemplate.has_key('primaryDataset'):
                    for outputModule in self.process.outputModules.values():
                        outputModule.dataset.primaryDataset = cms.untracked.string(moduleTemplate['primaryDataset'])

                # override compression level
##                if moduleTemplate.has_key('compressionLevel'):
##                    for outputModule in self.process.outputModules.values():
##                        outputModule.compressionLevel = cms.untracked.int32(moduleTemplate['compressionLevel'])

            else:
                logging.error("Neither output module details or template specified")
                
        elif params.has_key('processName'):

            if params.has_key('outputModuleDetails'):

                self.process = self.createProcessFromScratch(params['processName'],
                                                             configName = params.get('configName', 'auto-config'),
                                                             configVersion = params.get("configVersion", time.strftime("%d-%b-%Y-%H:%M:%S")))

            else:
                logging.error("No output module details specified")

        else:
            logging.error("Neither config file, framework config code or process name specified")

        # check if it worked
        if self.process == None:
            logging.error("Cannot build process, bailing out")
            loader.unload()
            return None

        # recreate source
        self.process.source = cms.Source(sourceType, fileNames = cms.untracked.vstring())

        # configure firstFreeID (works around a bug processing 2_0_X streamer files)
        if ( self.cmssw['version1'] == 2 and self.cmssw['version2'] == 0 ) \
           and sourceType == 'NewEventStreamFileReader':

            self.process.source.firstFreeID = cms.untracked.uint32(65536)

        # configure lazy download
        # (supported earlier than 2_1_8, but we don't use these releases anymore)
        if ( self.cmssw['version1'] > 2 ) \
           or ( self.cmssw['version1'] == 2 and self.cmssw['version2'] > 1 ) \
           or ( self.cmssw['version1'] == 2 and self.cmssw['version2'] == 1 and self.cmssw['version3'] >= 8 ):

            self.configureLazyDownload(
                params.get("enableLazyDownload", None) == True
                )

        # configure fastCloning and noEventSort
        # (supported earlier than 2_1_8, but we don't use these releases anymore)
        fastCloning = False
        if ( ( self.cmssw['version1'] > 2 ) \
           or ( self.cmssw['version1'] == 2 and self.cmssw['version2'] > 1 ) \
           or ( self.cmssw['version1'] == 2 and self.cmssw['version2'] == 1 and self.cmssw['version3'] >= 8 ) ) \
           and sourceType == 'PoolSource':

            fastCloning = True
            if params.get("noEventSort", None) == True:
                self.process.source.noEventSort = cms.untracked.bool(True)

        # add output modules
        if params.has_key('outputModuleDetails'):

            for moduleName, moduleDetails in params['outputModuleDetails'].items():
                logging.debug("Adding output module %s to workflow" % moduleName)
                self.addOutputModule(moduleName,
                                     moduleDetails['dataTier'],
                                     primaryDataset = moduleDetails.get("primaryDataset", None),
                                     selectEvents = moduleDetails.get("SelectEvents", None),
                                     setEventContentInOutput = params.get("setEventContentInOutput", False),
                                     compressionLevel = moduleDetails.get("compressionLevel", None))

        # apply generic modifiers to output modules
        # at the moment only fastCloning
        self.modifyOutputModules(fastCloning = fastCloning)

        cfgInterface = CMSSWConfig()
        loadedConfig = cfgInterface.loadConfiguration(self.process)
        loadedConfig.validateForProduction()

        # complete the output module info in workflow
        for moduleName, outMod in cfgInterface.outputModules.items():

            # easy for output modules we added
            if params.has_key("outputModuleDetails"):
                outMod.update(params["outputModuleDetails"][moduleName])

            # if we kept the output modules from the configs it's harder
            # need to combine info from template and config (processed dataset)
            elif params.has_key('outputModuleTemplate'):

                template = params["outputModuleTemplate"]
                outMod.update(template)

                if outMod.has_key("processingString"):
                    processingString = str(outMod["processingString"])
                elif outMod.has_key("filterName"):
                    processingString = str(outMod["filterName"])
                else:
                    processingString = None

                if processingString == None:
                    outMod['processedDataset'] = "%s-%s" % (template["acquisitionEra"],
                                                            template["processingVersion"])
                else:
                    outMod['processedDataset'] = "%s-%s-%s" % (template["acquisitionEra"],
                                                               processingString,
                                                               template["processingVersion"])

        if params.has_key("configFile"):
            cfgInterface.originalCfg = file(params['configFile']).read()

        loader.unload()
        
        return cfgInterface


    def configureLazyDownload(self, enableLazyDownload):
        """
        _configureLazyDownload_

        """
        import FWCore.ParameterSet.Config as cms

        if not self.process.services.has_key('AdaptorConfig'):
            self.process.add_(cms.Service('AdaptorConfig'))

        if enableLazyDownload:
            self.process.services["AdaptorConfig"].cacheHint = cms.untracked.string("lazy-download")
            self.process.services["AdaptorConfig"].readHint = cms.untracked.string("auto-detect")
            self.process.source.cacheSize = cms.untracked.uint32(100000000)
        else:
            self.process.services["AdaptorConfig"].cacheHint = cms.untracked.string("application-only")
            self.process.services["AdaptorConfig"].readHint = cms.untracked.string("direct-unbuffered")
            self.process.source.cacheSize = cms.untracked.uint32(0)

        return


    def createProcessFromFile(self, configFile, **params):
        """
        _createProcessFromScratch

        """
        import FWCore.ParameterSet.Config as cms
        
        cfgBaseName = os.path.basename(configFile).replace(".py", "")
        cfgDirName = os.path.dirname(configFile)
        modPath = imp.find_module(cfgBaseName, [cfgDirName])

        loadedConfig = imp.load_module(cfgBaseName, modPath[0],
                                       modPath[1], modPath[2])

        if params['stripOutputModules']:
            for moduleName in loadedConfig.process.outputModules.keys():
                del loadedConfig.process._Process__outputmodules[moduleName]

        return loadedConfig.process


    def createProcessFromFramework(self):
        """
        _createProcessFromScratch

        """
        import FWCore.ParameterSet.Config as cms



    def createProcessFromScratch(self, processName, **params):
        """
        _createProcessFromScratch

        """
        import FWCore.ParameterSet.Config as cms

        process = cms.Process(processName)
        process.include("FWCore/MessageLogger/data/MessageLogger.cfi")

        # for debugging, makes for short jobs
        #process.maxEvents = cms.untracked.PSet( input = cms.untracked.int32(10) )

        configName = params['configName']
        configVersion = params['configVersion']
        configAnnotation = "auto generated configuration"

        process.configurationMetadata = cms.untracked.PSet(
            name = cms.untracked.string(configName),
            version = cms.untracked.string(configVersion),
            annotation = cms.untracked.string(configAnnotation)
            )

        process.options = cms.untracked.PSet(
            Rethrow = cms.untracked.vstring("ProductNotFound","TooManyProducts","TooFewProducts"),
            wantSummary = cms.untracked.bool(False)
            )
            
        return process


    def addOutputModule(self, moduleName, dataTier, **params):
        """
        _addOutputModule_

        Add an output module and a dataset PSet to it

        """
        logging.info("Called addOutputModule with %s , %s , %s" % (moduleName, dataTier, params))

        import FWCore.ParameterSet.Config as cms

        outputModule = cms.OutputModule(
            "PoolOutputModule",
            fileName = cms.untracked.string("%s.root" % moduleName)
            )

        outputModule.dataset = cms.untracked.PSet(dataTier = cms.untracked.string(dataTier))

##        if params['compressionLevel'] != None:
##            outputModule.compressionLevel = cms.untracked.int32(params['compressionLevel'])

        if params['setEventContentInOutput']:
            if ( dataTier == 'ALCARECO' ) and \
               ( ( self.cmssw['version1'] > 3 ) \
                 or ( self.cmssw['version1'] == 3 and self.cmssw['version2'] > 2 ) \
                 or ( self.cmssw['version1'] == 3 and self.cmssw['version2'] == 2 and self.cmssw['version3'] >= 0 ) ):
                outputCommands = cms.untracked.vstring(
                    'drop *',
                    'keep edmTriggerResults_*_*_*',
                    'keep *_ALCARECOTkAlCosmicsCTF_*_*',
                    'keep *_ALCARECOTkAlCosmicsCosmicTF_*_*',
                    'keep *_ALCARECOTkAlCosmicsRS_*_*',
                    'keep *_eventAuxiliaryHistoryProducer_*_*',
                    'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
                    'keep L1MuGMTReadoutCollection_gtDigis_*_*',
                    'keep Si*Cluster*_si*Clusters_*_*',
                    'keep *_MEtoEDMConverter_*_*',
                    'keep *_ALCARECOTkAlCosmicsCTF_*_*',
                    'keep *_ALCARECOTkAlCosmicsCosmicTF_*_*',
                    'keep *_ALCARECOTkAlCosmicsRS_*_*',
                    'keep *_eventAuxiliaryHistoryProducer_*_*',
                    'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
                    'keep L1MuGMTReadoutCollection_gtDigis_*_*',
                    'keep Si*Cluster*_si*Clusters_*_*',
                    'keep *_MEtoEDMConverter_*_*',
                    'keep *_ALCARECOTkAlCosmics*0T_*_*',
                    'keep *_eventAuxiliaryHistoryProducer_*_*',
                    'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
                    'keep L1MuGMTReadoutCollection_gtDigis_*_*',
                    'keep Si*Cluster*_si*Clusters_*_*',
                    'keep *_MEtoEDMConverter_*_*',
                    'keep *_ALCARECOTkAlCosmics*0T_*_*',
                    'keep *_eventAuxiliaryHistoryProducer_*_*',
                    'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
                    'keep L1MuGMTReadoutCollection_gtDigis_*_*',
                    'keep Si*Cluster*_si*Clusters_*_*',
                    'keep *_MEtoEDMConverter_*_*',
                    'keep *_ALCARECOSiStripCalZeroBias_*_*',
                    'keep *_calZeroBiasClusters_*_*',
                    'keep *_MEtoEDMConverter_*_*',
                    'keep HOCalibVariabless_*_*_*',
                    'keep *_ALCARECOMuAlStandAloneCosmics_*_*',
                    'keep *_muonCSCDigis_*_*',
                    'keep *_muonDTDigis_*_*',
                    'keep *_muonRPCDigis_*_*',
                    'keep *_dt1DRecHits_*_*',
                    'keep *_dt2DSegments_*_*',
                    'keep *_dt4DSegments_*_*',
                    'keep *_csc2DRecHits_*_*',
                    'keep *_cscSegments_*_*',
                    'keep *_rpcRecHits_*_*',
                    'keep *_ALCARECOMuAlGlobalCosmics_*_*',
                    'keep *_muonCSCDigis_*_*',
                    'keep *_muonDTDigis_*_*',
                    'keep *_muonRPCDigis_*_*',
                    'keep *_dt1DRecHits_*_*',
                    'keep *_dt2DSegments_*_*',
                    'keep *_dt4DSegments_*_*',
                    'keep *_csc2DRecHits_*_*',
                    'keep *_cscSegments_*_*',
                    'keep *_rpcRecHits_*_*',
                    'keep *_ALCARECOMuAlCalIsolatedMu_*_*',
                    'keep *_muonCSCDigis_*_*',
                    'keep *_muonDTDigis_*_*',
                    'keep *_muonRPCDigis_*_*',
                    'keep *_dt1DRecHits_*_*',
                    'keep *_dt2DSegments_*_*',
                    'keep *_dt4DSegments_*_*',
                    'keep *_csc2DRecHits_*_*',
                    'keep *_cscSegments_*_*',
                    'keep *_rpcRecHits_*_*',
                    'keep *_muonDTDigis_*_*',
                    'keep CSCDetIdCSCWireDigiMuonDigiCollection_*_*_*',
                    'keep CSCDetIdCSCStripDigiMuonDigiCollection_*_*_*',
                    'keep DTLayerIdDTDigiMuonDigiCollection_*_*_*',
                    'keep *_dt4DSegments_*_*',
                    'keep *_cscSegments_*_*',
                    'keep *_rpcRecHits_*_*',
                    'keep RPCDetIdRPCDigiMuonDigiCollection_*_*_*',
                    'keep recoMuons_muonsNoRPC_*_*',
                    'keep L1MuRegionalCands_*_RPCb_*',
                    'keep L1MuRegionalCands_*_RPCf_*',
                    'keep L1MuGMTCands_*_*_*',
                    'keep L1MuGMTReadoutCollection_*_*_*'),
##                outputModule.outputCommands = cms.untracked.vstring(
##                    'drop *',
##                    'keep edmTriggerResults_*_*_*',
##                    'keep *_ALCARECOMuAlStandAloneCosmics_*_*',
##                    'keep *_ALCARECOMuAlGlobalCosmics_*_*',
##                    'keep *_ALCARECOMuAlCalIsolatedMu_*_*',
##                    'keep *_cosmicMuons_*_*',
##                    'keep *_cosmictrackfinderP5_*_*',
##                    'keep Si*Cluster*_*_*_*',
##                    'keep *_muonCSCDigis_*_*',
##                    'keep *_muonDTDigis_*_*',
##                    'keep *_muonRPCDigis_*_*',
##                    'keep *_dt1DRecHits_*_*',
##                    'keep *_dt2DSegments_*_*',
##                    'keep *_dt4DSegments_*_*',
##                    'keep *_csc2DRecHits_*_*',
##                    'keep *_cscSegments_*_*',
##                    'keep *_rpcRecHits_*_*',
##                    'keep HOCalibVariabless_*_*_*',
##                    'keep *_ALCARECOTkAlCosmicsCTF_*_*',
##                    'keep *_ALCARECOTkAlCosmicsCosmicTF_*_*',
##                    'keep *_ALCARECOTkAlCosmicsRS_*_*',
##                    'keep *_ALCARECOTkAlCosmics*0T_*_*',
##                    'keep *_eventAuxiliaryHistoryProducer_*_*',
##                    'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
##                    'keep *_MEtoEDMConverter_*_*',
##                    'keep CSCDetIdCSCWireDigiMuonDigiCollection_*_*_*',
##                    'keep CSCDetIdCSCStripDigiMuonDigiCollection_*_*_*',
##                    'keep DTLayerIdDTDigiMuonDigiCollection_*_*_*',
##                    'keep RPCDetIdRPCDigiMuonDigiCollection_*_*_*',
##                    'keep L1MuGMTCands_*_*_*',
##                    'keep L1MuGMTReadoutCollection_*_*_*')
            elif dataTier == 'FEVTHLTALL':
                installEventContent(self.process, "FEVT", outputModule)
                outputModule.outputCommands.append('keep *_*_*_HLT')
            else:
                installEventContent(self.process, dataTier, outputModule)

        if params['primaryDataset'] !=  None:
            outputModule.dataset.primaryDataset = cms.untracked.string(params['primaryDataset'])

        if params['selectEvents'] != None:
            outputModule.SelectEvents = cms.untracked.PSet(
                SelectEvents = cms.vstring()
                )
            for selCond in params['selectEvents']:
                outputModule.SelectEvents.SelectEvents.append(selCond)

        setattr(self.process, moduleName, outputModule)

        return


    def modifyOutputModules(self, **params):
        """
        _modifyOutputModules)

        Apply generic modifiers to output modules

        At the moment that's only fastCloning

        """
        import FWCore.ParameterSet.Config as cms

        if params.has_key('fastCloning'):
            for outputModule in self.process.outputModules.values():
                outputModule.fastCloning = cms.untracked.bool(params['fastCloning'])

        if params.has_key('primaryDataset'):
            for outputModule in self.process.outputModules.values():
                outputModule.fastCloning = cms.untracked.bool(params['primaryDataset'])

        return


    def getLFN(self, outputModule, **options):
        """
        _getLFN_

        Build a default name structure based on the parameters passed.

        """
        if outputModule.get("acquisitionEra") == None:
            msg = "Acquistion Era is not specified!!!\n"
            raise RuntimeError, msg

        #lfn = "/T0/hufnagel/recotest/store/"
        lfn = "/store/"
        if options.get("Unmerged", False):
            lfn += "temp/"
        lfn += options.get("dataType","data")
        lfn += "/"
        lfn += "%s/" % outputModule.get("acquisitionEra")
        lfn += "%s/" % outputModule.get("primaryDataset")
        lfn += "%s/" % outputModule.get("dataTier")
        lfn += "%s/" % outputModule.get("processingVersion")

        runString = str(self.run).zfill(9)
        lfn += "%s/%s/%s" % (runString[0:3],
                             runString[3:6],
                             runString[6:9])
        lfn += "/"

        return lfn

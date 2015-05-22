#!/usr/bin/env python
"""
_RunConfig_

Object containing the configuration details for a single run.
"""

__revision__ = "$Id: RunConfig.py,v 1.86 2009/07/18 15:23:12 hufnagel Exp $"
__version__ = "$Revision: 1.86 $"

import logging
import urllib
import os
import base64

from T0.DataStructs.DatasetDefinition import DatasetDefinition

from T0.State.Database.Reader import ListDatasets
from T0.State.Database.Reader import ListRunConfig
from T0.State.Database.Reader import ListRuns

from WMCore.Configuration import ConfigSection

class RunConfig:
    """
    _RunConfig_

    Container object for the configuration of a single run


    Only quantity that is passed from outside
      runNumber: The run number this configuration applies to.

    These are hard coded, some should be discovered
      acquisitionEra : Era of data taking

    The other quantities are retrieved from T0AST on demand 
      repackCMSSW: The CMSSW version that should be used for repacking.
      process: Online HLT process name, needed to create SelectEvents PSet values
      startTime : Run start time (needed by export system)
      streamToDatasets : Dictionary mapping stream ids to dataset ids
      datasetToTriggers : Dictionary mapping dataset ids to trigger path names
      primaryDatasets : Dictionary mapping dataset id to name
      streams : Dictionary mapping stream id to name

      t0RepackConfig : Dictionary mapping datasetID to config parameter value dict
      t0RcoConfig : Dictionary mapping datasetID to config parameter value dict
      t0AlcaConfig : Dictionary mapping datasetID to config parameter value dict
      t0DQMConfig : Dictionary mapping datasetID to config parameter value dict
      scenario : Maps datasetID to a scenario.
    """
    def __init__(self, dbConn, runNumber, configCache):

        self.t0astDBConn = dbConn
        self.runNumber = runNumber
        
        self.acquisitionEra = None

        self.repackCMSSW = None
        self.expressCMSSW = None
        self.process = None
        self.startTime = None

        self.streamToDatasets = None
        self.datasetToTriggers = None
        self.primaryDatasets = None
        self.streams = None

        self.configCacheDir = configCache

        self.t0RepackConfig = None
        self.t0RecoConfig = None
        self.t0AlcaConfig = None
        self.t0DQMConfig = None
        self.t1SkimConfig = None
        self.expressConfig = None
        self.wmbsPublishConfig = None

        self.processingStyles = None
        self.scenario = None

    def t0astLoadRunInfo(self):
        """
        _t0astLoadRunInfo_

        Loads run information from T0AST into memory
        """
        if self.repackCMSSW == None:
            runInfo = ListRunConfig.retrieveRunInfo(self.t0astDBConn,
                                                    self.runNumber)
            self.repackCMSSW = runInfo[0][0]
            self.expressCMSSW = runInfo[0][1]
            self.process = runInfo[0][2]
            self.startTime = runInfo[0][3]
            self.acquisitionEra = runInfo[0][4]
    
            logging.debug("RunConfig : runInfo = %s" % runInfo)
        return True


    def t0astLoadStreamToDatasets(self):
        """
        _t0astLoadStreamToDatasets_

        Loads stream to dataset mapping from T0AST into memory
        """
        if self.streamToDatasets == None:
            self.streamToDatasets = ListRunConfig.retrieveStreamToDatasets(self.t0astDBConn,
                                                                              self.runNumber)
            logging.debug("RunConfig : streamToDatasets = %s" % self.streamToDatasets)
            
        return True


    def t0astLoadDatasetToTriggers(self):
        """
        _t0astLoadDatasetToTriggers_

        Load dataset to trigger path mapping from T0AST into memory
        """
        if self.datasetToTriggers == None:
            self.datasetToTriggers = ListRunConfig.retrieveDatasetToTriggers(self.t0astDBConn,
                                                                             self.runNumber)
            logging.debug("RunConfig : datasetToTriggers = %s" % self.datasetToTriggers)
        return True
        

    def t0astLoadPrimaryDatasets(self):
        """
        _t0astLoadPrimaryDatasets_

        Loads primary dataset id to name mapping from T0AST into memory
        """
        if self.primaryDatasets == None:
            self.primaryDatasets = ListRunConfig.retrievePrimaryDatasets(self.t0astDBConn,
                                                                         self.runNumber)
            logging.debug("RunConfig : primaryDatasets = %s" % self.primaryDatasets)
        return True


    def t0astLoadStreams(self):
        """
        _t0astLoadStreams_
    
        Loads stream id to name mappings for T0AST into memory
        """
        if self.streams == None:
            self.streams = ListRunConfig.retrieveStreams(self.t0astDBConn,
                                                         self.runNumber)
            logging.debug("RunConfig : streams = %s" % self.streams)
        return True


    def t0astLoadScenarioConfiguration(self):
        """
        _t0astLoadScenarioConfiguration_
    
        Loads all the scenarios to for T0AST into memory
        """
        if self.scenario == None:
            self.scenario = ListRunConfig.retrieveScenario(self.t0astDBConn,
                                                           self.runNumber)
            logging.debug("RunConfig : scenario = %s" % self.scenario)
        return True

    
    def t0astLoadRepackConfiguration(self):
        """
        _t0astLoadT0RepackConfiguration_
    
        Load configuration information that's relevant to repacking into
        memory.
        """
        if self.t0RepackConfig == None:
            self.t0RepackConfig = ListRunConfig.retrieveRepackConfig(self.t0astDBConn,
                                                                     self.runNumber)            
        return True

    def t0astLoadRecoConfiguration(self):
        """
        _t0astLoadRecoConfiguration_
    
        Loads Reco config for T0AST into memory
        """
        if self.t0RecoConfig == None:
            self.t0RecoConfig = ListRunConfig.retrieveRecoConfig(self.t0astDBConn,
                                                                 self.runNumber)
        return True


    def t0astLoadAlcaConfiguration(self):
        """
        _t0astLoadAlcaConfig_
    
        Loads Alca Reco config for T0AST into memory
        """
        if self.t0AlcaConfig == None:
            self.t0AlcaConfig = ListRunConfig.retrieveAlcaConfig(self.t0astDBConn,
                                                                 self.runNumber)
        return True


    def t0astLoadWMBSPublishConfiguration(self):
        """
        _t0astLoadWMBSPublishConfig_
    
        Loads WMBSPublish config for T0AST into memory
        """
        if self.wmbsPublishConfig == None:
            self.wmbsPublishConfig = ListRunConfig.retrieveWMBSPublishConfig(self.t0astDBConn,
                                                                 self.runNumber)
        return True


    def t0astLoadDQMConfiguration(self):
        """
        _t0astLoadDQMConfiguration_
    
        Loads DQM config for T0AST into memory
        """
        if self.t0DQMConfig == None:
            self.t0DQMConfig = ListRunConfig.retrieveDQMConfig(self.t0astDBConn,
                                                               self.runNumber)
        return True


    def t0astLoadT1SkimConfiguration(self):
        """
        _t0astLoadT1SkimConfiguration_

        Loads the Tier1 Skim configuration from T0AST into memory.
        """
        if self.t1SkimConfig == None:
            self.t1SkimConfig = ListRunConfig.retrieveTier1SkimConfig(self.t0astDBConn,
                                                                      self.runNumber)

        return True


    def t0astLoadProcessingStyles(self):
        """
        _t0astLadProcessingStyles_

        Loads the processing styles for each stream.
        """
        if self.processingStyles == None:
            self.processingStyles = ListRunConfig.retrieveProcessingStyles(self.t0astDBConn,
                                                                           self.runNumber)

        return True

    def t0astLoadExpressConfiguration(self):
        """
        _t0astLoadExpressConfiguration_

        Loads the express configuration from T0AST into memory.
        """
        if self.expressConfig == None:
            self.expressConfig = ListRunConfig.retrieveExpressConfig(self.t0astDBConn,
                                                                     self.runNumber)

        return True

    def getScenarioConfiguration(self, primaryDatasetID):
        """
        _getSenarioConfiguration_
        
        Retrieve the scenario that has been assigned to a dataset.
        """
        if not self.t0astLoadScenarioConfiguration():
            return None

        return self.scenario[primaryDatasetID]

    def getRepackConfiguration(self, datasetPathID):
        """
        _getRepackConfiguration_
        
        @param datasetPathID: dataset path id
        
        Retrieve the repacking configuration for a particular dataset, which
        will consist of a ConfigSection with the follow attributes:
          ProcessingVersion - The processing version that is to be used for
                              data produced for this dataset.
        """
        
        datasetIDs = ListDatasets.listDatasetIDsFromID(self.t0astDBConn,
                                                        datasetPathID)
        
        return self.getRepackConfigurationByPrimaryID(datasetIDs["PRIMARY"])
        
    def getRepackConfigurationByPrimaryID(self, primaryDatasetID):
        """
        _getRepackConfigurationByPrimaryID_
        
        @param primaryDatasetID: primary dataset id
        
        Retrieve the repacking configuration for a particular dataset, which
        will consist of a ConfigSection with the follow attributes:
          ProcessingVersion - The processing version that is to be used for
                              data produced for this dataset.
        """
        if not self.t0astLoadRepackConfiguration():
            return None        

        repackConfig = self.t0RepackConfig[primaryDatasetID]

        confSection = ConfigSection("Repacker")
        confSection.ProcessingVersion = repackConfig["PROC_VER"]
        return confSection

    def getRecoConfiguration(self, datasetPathID):
        """
        _getRecoConfiguration_

        @param datasetPathID: dataset path id
        
        Retrieve the reconstruction configuration for a particular dataset,
        which will consist of a ConfigSection with the follow attributes:
          DoReco - Boolean that determines whether or not reconstruction is to
                   be preformed on this dataset.
          GlobalTag - The global tag to be used for prompt reconstuction.
          CMSSWVersion - String containing the framework version to use for
                         reconstruction.
          ConfigURL - A URL pointing at the python framework configuration to
                      be used for reconstruction.
          ProcessingVersion - The processing version that is to be used for
                              data produced for this dataset.
        """
        
        datasetIDs = ListDatasets.listDatasetIDsFromID(self.t0astDBConn,
                                                       datasetPathID)
        return self.getRecoConfigurationByPrimaryID(datasetIDs["PRIMARY"])
    
    def getRecoConfigurationByPrimaryID(self, primaryDatasetID):
        """
        _getRecoConfigurationByPrimaryID_

        @param primaryDatasetID: primary dataset id
                
        Retrieve the reconstruction configuration for a particular dataset,
        which will consist of a ConfigSection with the follow attributes:
          DoReco - Boolean that determines whether or not reconstruction is to
                   be preformed on this dataset.
          GlobalTag - The global tag to be used for prompt reconstuction.
          CMSSWVersion - String containing the framework version to use for
                         reconstruction.
          ConfigURL - A URL pointing at the python framework configuration to
                      be used for reconstruction.
          ProcessingVersion - The processing version that is to be used for
                              data produced for this dataset.
        """
        if not self.t0astLoadRecoConfiguration():
            return None

        recoConfig = self.t0RecoConfig[primaryDatasetID]
        
        confSection = ConfigSection("Reconstruction")
        confSection.DoReco = recoConfig["DO_RECO"]
        confSection.GlobalTag = recoConfig["GLOBALTAG"]
        confSection.CMSSWVersion = recoConfig["CMSSW_VERSION"]
        confSection.ConfigURL = recoConfig["CONFIG_URL"]
        confSection.ProcessingVersion = recoConfig["PROC_VER"]
        return confSection
       

    def getAlcaConfiguration(self, datasetPathID):
        """
        _getAlcaConfiguration_
        
        @param datasetPathID: dataset path id
        
        Retrieve the alca configuration for a particular dataset, which will
        consist of a ConfigSection with the follow attributes:
          DoAlco - Boolean that determines whether or not alca skims are to
                   be preformed on this dataset.
          CMSSWVersion - String containing the framework version to use for
                         the alca skims.
          ConfigURL - A URL pointing at the python framework configuration to
                      be used for the alca skims.
          ProcessingVersion - The processing version that is to be used for
                              data produced for this dataset.
        """

        datasetIDs = ListDatasets.listDatasetIDsFromID(self.t0astDBConn,
                                                       datasetPathID)

        return self.getAlcaConfigurationByPrimaryID(datasetIDs["PRIMARY"])
    
    def getAlcaConfigurationByPrimaryID(self, primaryDatasetID):
        """
        _getAlcaConfiguration_
        
        @param primaryDatasetID: primary dataset id
        
        Retrieve the alca configuration for a particular dataset, which will
        consist of a ConfigSection with the follow attributes:
          DoAlco - Boolean that determines whether or not alca skims are to
                   be preformed on this dataset.
          CMSSWVersion - String containing the framework version to use for
                         the alca skims.
          ConfigURL - A URL pointing at the python framework configuration to
                      be used for the alca skims.
          ProcessingVersion - The processing version that is to be used for
                              data produced for this dataset.
        """
        if not self.t0astLoadAlcaConfiguration():
            return None        

        alcaConfig = self.t0AlcaConfig[primaryDatasetID]

        confSection = ConfigSection("Alca")
        confSection.DoAlca = alcaConfig["DO_ALCA"]
        confSection.CMSSWVersion = alcaConfig["CMSSW_VERSION"]
        confSection.ConfigURL = alcaConfig["CONFIG_URL"]
        confSection.ProcessingVersion = alcaConfig["PROC_VER"]
        return confSection
    

    def getWMBSPublishConfiguration(self, datasetPathID):
        """
        _getWMBSPublishConfiguration_
        
        @param datasetPathID: dataset path id
        
        Retrieve the WMBS Publish configuration for a particular dataset,
        which will consist of a ConfigSection with the follow attributes:
          DoWMBSPub - Boolean that determines whether or not to publish
                      parts of this dataset.
          DataTiersTo - Data Tiers to publish for this dataset to where
        """

        datasetIDs = ListDatasets.listDatasetIDsFromID(self.t0astDBConn,
                                                       datasetPathID)

        wmbsPublishConfig = self.getWMBSPublishConfigurationByPrimaryID(datasetIDs["PRIMARY"])
        dataTiers = []
        for dataTierTo in wmbsPublishConfig.DataTiersTo:
            dataTierList = dataTierTo.split('@')
            dataTiers.extend( dataTierList[0].split(',') )
        dataTierIDs = []
        for dataTier in dataTiers:
            dataTierIDs.append( int( dataTier ) )
        if datasetIDs["TIER"] not in dataTierIDs:
            wmbsPublishConfig.DoWMBSPublish = False

        return wmbsPublishConfig
    
    def getWMBSPublishConfigurationByPrimaryID(self, primaryDatasetID):
        """
        _getWMBSPublishConfiguration_
        
        @param primaryDatasetID: primary dataset id
        
        Retrieve the WMBS Publish configuration for a particular dataset,
        which will consist of a ConfigSection with the follow attributes:
          DoWMBSPub - Boolean that determines whether or not to publish
                      parts of this dataset.
          DataTiersTo - Data Tiers to publish for this primary dataset
                        and to where
        """
        if not self.t0astLoadWMBSPublishConfiguration():
            return None        

        wmbsPublishConfig = self.wmbsPublishConfig[primaryDatasetID]

        confSection = ConfigSection("WMBSPublish")
        confSection.DoWMBSPublish = wmbsPublishConfig["DO_WMBS_PUBLISH"]
        confSection.DataTiersTo = wmbsPublishConfig["DATA_TIERS_TO"]
        return confSection

    def getDQMConfiguration(self, datasetPathID):
        """
        _getDQMConfiguration_
        
        @param datasetPathID: dataset path id
        
        Retrieve the dqm configuration for a particular dataset, which will
        consist of a ConfigSection with the follow attributes:
          DoAlco - Boolean that determines whether or not dqm jobs are to
                   be run on this dataset.
          CMSSWVersion - String containing the framework version to use for
                         the dqm jobs.
          ConfigURL - A URL pointing at the python framework configuration to
                      be used for the dqm jobs.
          ProcessingVersion - The processing version that is to be used for
                              data produced for this dataset.        
        """
        
        datasetIDs = ListDatasets.listDatasetIDsFromID(self.t0astDBConn,
                                                       datasetPathID)
        
        return self.getDQMConfigurationByPrimaryID(datasetIDs["PRIMARY"])
    
    def getDQMConfigurationByPrimaryID(self, primaryDatasetID):
        """
        _getDQMConfiguration_
        
        @param primaryDatasetID: primary dataset id
        
        Retrieve the dqm configuration for a particular dataset, which will
        consist of a ConfigSection with the follow attributes:
          DoAlco - Boolean that determines whether or not dqm jobs are to
                   be run on this dataset.
          CMSSWVersion - String containing the framework version to use for
                         the dqm jobs.
          ConfigURL - A URL pointing at the python framework configuration to
                      be used for the dqm jobs.
          ProcessingVersion - The processing version that is to be used for
                              data produced for this dataset.        
        """
        if not self.t0astLoadDQMConfiguration():
            return None        
        
        dqmConfig = self.t0DQMConfig[primaryDatasetID]

        confSection = ConfigSection("DQM")
        confSection.DoDQM = dqmConfig["DO_DQM"]
        confSection.CMSSWVersion = dqmConfig["CMSSW_VERSION"]
        confSection.ConfigURL = dqmConfig["CONFIG_URL"]
        confSection.ProcessingVersion = dqmConfig["PROC_VER"]
        return confSection
    
    def getSkimConfiguration(self, primaryDatasetID, dataTierID):
        """
        _getSkimConfiguration_

        Retrieve the configurations of any Tier1 Skims that are defined for a
        given primary dataset and data tier.  If no skims are defined then None
        will be returned.  A list of ConfigSections with the following
        attributes will be returned if skims have been defined:
          SkimName - The name of the skim.
          CMSSWVersion - The framework version to be used to run the skims.
          ConfigURL - The URL to the framework configuration.
          ProcessingVersion - The processing version for the skim.
          SiteName - The name of the site to run the skim at.
        """
        self.t0astLoadT1SkimConfiguration()

        if primaryDatasetID not in self.t1SkimConfig.keys():
            return None
        if dataTierID not in self.t1SkimConfig[primaryDatasetID].keys():
            return None
        if len(self.t1SkimConfig[primaryDatasetID][dataTierID]) == 0:
            return None

        confSections = []
        for skim in self.t1SkimConfig[primaryDatasetID][dataTierID]:
            confSection = ConfigSection("Tier1Skim")
            confSection.SkimName = skim["SKIM_NAME"]
            confSection.CMSSWVersion = skim["CMSSW_VERSION"]
            confSection.ConfigURL = skim["CONFIG_URL"]
            confSection.ProcessingVersion = skim["PROC_VER"]
            confSection.TwoFileRead = skim["TWO_FILE_READ"]
            confSections.append(confSection)

        return confSections


    def getExpressConfiguration(self, streamID):
        """
        _getExpressConfiguration_

        Retrieve the express configuration for a given stream id.  If no express
        configuration is defined then None will be returned.  A ConfigSection
        with the following attributes will be returned if skims have been
        defined:
          StreamID - ID of the stream that this config corresponds to.
          CMSSWVersion - The framework version to be used to run the skims.
          ProcessingConfigURL - The URL to the processing framework
                                configuration.
          AlcaMergeConfigURL - The URL to the merge packing framework
                                  configuration.
          SplitInProcessing - Boolean that determines whether datasets are
                              split out in the processing step or the merge
                              step.
          ProcessingVersion - The processing version of the output of the
                              express stream. 
        """
        if not self.t0astLoadRunInfo():
            return None
        if not self.t0astLoadExpressConfiguration():
            return None

        config = self.expressConfig.get(streamID, None)
        if config == None:
            return None

        confSection = ConfigSection("ExpressConfig")
        confSection.StreamID = streamID
        confSection.CMSSWVersion = self.expressCMSSW
        confSection.GlobalTag = config["GLOBAL_TAG"]
        confSection.ProcessingConfigURL = config["PROC_URL"]
        confSection.SplitInProcessing = bool(config["SPLIT_IN_PROC"])
        confSection.ProcessingVersion = config["PROC_VER"]
        confSection.AlcaMergeConfigURL = config["ALCAMERGE_URL"]
        confSection.DataTiers = config["DATATIERS"]

        return confSection


    def getRunNumber(self):
        """
        _runNumber_

        Return the run number
        """
        return self.runNumber


    def repackCMSSWVersion(self):
        """
        _repackCMSSWVersion_

        Return the framework version that should be used for repacking.
        This will take into account any version mappings that have been setup.

        """
        if not self.t0astLoadRunInfo():
            return None

        return self.repackCMSSW


    def getAcquisitionEra(self):
        """
        _getAcquisitionEra_

        Return the acquisition era
        """
        if not self.t0astLoadRunInfo():
            return None        
        
        return self.acquisitionEra
    

    def getStartTime(self):
        """
        _getStartTime_

        Return the start time for this run.
        """
        if not self.t0astLoadRunInfo():
            return None

        return self.startTime


    def getDatasetsForStream(self, streamID):
        """
        _getDatasetsForStream_

        Return a list of dataset ids for a given stream
        """
        if not self.t0astLoadStreamToDatasets():
            return None

        return self.streamToDatasets.get(streamID, None)


    def getTriggersForDataset(self, datasetID):
        """
        _getTriggersForDataset_

        Return a list of triggers for a given dataset id
        """
        if not self.t0astLoadDatasetToTriggers():
            return None

        return self.datasetToTriggers.get(datasetID, None)


    def getPrimaryDatasets(self):
        """
        _primaryDatasets_

        Return a dictionary with all datasets ids and names used for this run.
        The ids are the keys, names are values

        """
        if not self.t0astLoadPrimaryDatasets():
            return None
        
        return self.primaryDatasets

    
    def getPrimaryDatasetID(self, dataset):
        """
        _primaryDatasetID_

        Return an id for a primary dataset name
        """
        if not self.t0astLoadPrimaryDatasets():
            return None

        for n,i in self.primaryDatasets.iteritems():
            if (i == dataset):
                return n

        return None


    def getPrimaryDatasetName(self, datasetID):
        """
        _primaryDatasetName_

        Return a name for a primary dataset id
        """
        if not self.t0astLoadPrimaryDatasets():
            return None

        return self.primaryDatasets.get(datasetID, None)


    def getStreamID(self, streamName):
        """
        _getStreamID_

        Return an id for a stream name
        """
        if not self.t0astLoadStreams():
            return None

        for i,n in self.streams.items():
            if (n == streamName):
                return i

        return None


    def getStreamName(self, streamID):
        """
        _getStreamName_

        Return a name for a stream id
        """
        if not self.t0astLoadStreams():
            return None

        return self.streams.get(streamID, None)


    # external method, used by RepackerInjector, ExpressInjector and ExpressMerger
    def getOutputModuleInfoList(self, processType, streamID, dataTiers, splitIntoDatasets = False):
        """
        _getOutputModuleInfoList_

        List of DatasetDefinition (stream, dataset, [trigerPathList]) for a given stream only

        """
        if not self.t0astLoadRunInfo():
            return None
        if not self.t0astLoadStreamToDatasets():
            return None
        if not self.t0astLoadDatasetToTriggers():
            return None
        if not self.t0astLoadPrimaryDatasets():
            return None
        if not self.t0astLoadRepackConfiguration():
            return None
        if not self.t0astLoadExpressConfiguration():
            return None

        streamName = self.getStreamName(streamID)
        if streamName == None:
            msg = "RunConfig fatal error : No streamID %s in run %s\n" % (streamID, self.runNumber)
            raise RuntimeError, msg

        datasets = self.streamToDatasets.get(streamID, None)
        if datasets == None:
            msg = "RunConfig fatal error : No datasets defined for stream %s in run %s\n" % (streamName, self.runNumber)
            raise RuntimeError, msg

        # support passing a single dataTier
        if type(dataTiers) != list:
            dataTiers = [dataTiers]

        outputModuleInfoList = []

        if splitIntoDatasets:
            for datasetID in datasets:
                for dataTier in dataTiers:
                    datasetDef = self.getOutputModuleInfo(processType, dataTier,
                                                          streamID = streamID,
                                                          datasetID = datasetID,
                                                          selectOnTriggers = True)
                    outputModuleInfoList.append(datasetDef)
        else:
            for dataTier in dataTiers:
                datasetDef = self.getOutputModuleInfo(processType, dataTier,
                                                      streamID = streamID)
                outputModuleInfoList.append(datasetDef)

        return outputModuleInfoList


    # used internally (by getOutputModuleInfoList)
    # used externally (any workflow that has only one or a few output modules that are retrieved individually)
    def getOutputModuleInfo(self, processType, dataTier, **options):
        """
        _getOutputModuleInfo_

        Returns all relevant information for an output module
        for the given stream and/or dataset and data tier

        Later passed to the workflow creator and used to build
        the workflow and the CMSSW configuration

        """
        streamID = options.get("streamID", None)
        datasetID = options.get("datasetID", None)

        # need either datasetID or streamID
        if streamID == None:
            if datasetID == None:
                msg = "RunConfig fatal error : Either stream or dataset required parameter"
                raise RuntimeError, msg
            else:
                streamID = self.getStreamIDfromDatasetID(datasetID)
        else:
            # if both are supplied, check consistency
            if datasetID != None:
                if streamID != self.getStreamIDfromDatasetID(datasetID):
                    msg = "RunConfig fatal error : DatasetID %d, StreamID %s bad match for run %s\n" % (datasetID, streamID, self.runNumber)
                    raise RuntimeError, msg

        # get stream name
        streamName = self.getStreamName(streamID)
        if streamName == None:
            msg = "RunConfig fatal error : No streamID %s in run %s\n" % (streamID, self.runNumber)
            raise RuntimeError, msg

        datasetDef = DatasetDefinition()
        datasetDef["stream"] = streamName
        datasetDef["dataTier"] = dataTier

        datasetDef["process"] = self.process
        datasetDef["acquisitionEra"] = self.acquisitionEra

        if datasetID != None:
            datasetName = self.getPrimaryDatasetName(datasetID)
            if datasetName == None:
                msg = "RunConfig fatal error : No datasetID %s in run %s\n" % (datasetID, self.runNumber)
                raise RuntimeError, msg
            datasetDef["dataset"] = datasetName
            if options.get("selectOnTriggers", False):
                triggerPaths = self.datasetToTriggers.get(datasetID, None)
                if triggerPaths == None:
                    msg = "RunConfig fatal error: No triggers for dataset %s in run %s\n" % (datasetName, self.runNumber)
                    raise RuntimeError, msg
                datasetDef["triggerPaths"] = triggerPaths
        else:
            datasetDef["dataset"] = "Stream%s" % streamName

        procString = self.getProcessingString(processType)
        procVersion = self.getProcessingVersion(processType, streamID = streamID, datasetID = datasetID)

        if procVersion == None:
            msg = "RunConfig fatal error: Cannot determine processing version"
            raise RuntimeError, msg

        if procString == None:
            datasetDef["processedDataset"] = "%s-%s" % (self.acquisitionEra, procVersion)
        else:
            datasetDef["processedDataset"] = "%s-%s-%s" %(self.acquisitionEra, procString, procVersion)

        datasetDef["processingVersion"] = procVersion

        return datasetDef


    # DEPRECATED
    # investigate why it's used anywhere, should not be needed
    def processedDataset(self, processType, datasetID):
        """
        _processedDataset_

        Generate a processed dataset name for a particular processType and primary
        dataset.  This currently supports the following processTypes and will
        generate processed dataset names for the processes with the form:
          Repacker   - AcquisitionEra-ProcessingVersion
          PromptReco - AcquisitionEra-PromptReco-ProcessingVersion
          AlcaSkim   - AcquisitionEra-AlcaSkim-ProcessingVersion
          Express    - AcquisitionEra-Express-ProcessingVersion
        """
        procString = self.getProcessingString(processType)
        procVersion = self.getProcessingVersion(processType, datasetID = datasetID)

        if procVersion == None:
            msg = "RunConfig fatal error: Cannot determine processing version"
            raise RuntimeError, msg

        if procString == None:
            return "%s-%s" % (self.acquisitionEra, procVersion)
        else:
            return "%s-%s-%s" %(self.acquisitionEra, procString, procVersion)


    # used only internally
    def getProcessingString(self, processType):
        """
        _getProcessingString_

        Retrieve processing string based on processType and dataset

        """
        if processType == "Repacker":
            return None
        elif processType == "PromptReco":
            return "PromptReco"
        elif processType == "AlcaSkim":
            return "AlcaSkim"
        elif processType == "Express":
            return "Express"
        else:
            logging.error("RunConfig: Unknown processType: %s" % process)
            return None


    # used only internally
    def getProcessingVersion(self, processType, **options):
        """
        _getProcessingVersion_

        Retrieve processing version based on processType and dataset

        """
        if self.acquisitionEra == None:
            self.t0astLoadRunInfo()

        streamID = options.get("streamID", None)
        datasetID = options.get("datasetID", None)

        if processType == "Repacker":
            if datasetID == None:
                return None
            if not self.t0astLoadRepackConfiguration():
                return None
            return self.t0RepackConfig[datasetID]["PROC_VER"]
        elif processType == "PromptReco":
            if datasetID == None:
                return None
            if not self.t0astLoadRecoConfiguration():
                return None
            return self.t0RecoConfig[datasetID]["PROC_VER"]
        elif processType == "AlcaSkim":
            if datasetID == None:
                return None
            if not self.t0astLoadAlcaConfiguration():
                return None
            return self.t0AlcaConfig[datasetID]["PROC_VER"]
        elif processType == "Express":
            if streamID == None:
                return None
            if not self.t0astLoadExpressConfiguration():
                return None
            config = self.expressConfig.get(streamID, None)
            if config == None:
                return None
            else:
                return config["PROC_VER"]
        else:
            logging.error("Unknown processType: %s" % process)
            return None


    def retrieveConfigFromURL(self, configURL):
        """
        _retrieveConfigFromURL_

        Given a URL to a config file, retrieve that config and store it on
        local disk.  This function will then return the path to the locally
        stored copy of the config.  This function will check to see if the
        config already exists on local disk and if so will just return the
        path to the already retrieved file.
        """
        if not os.path.isdir(self.configCacheDir):
            os.makedirs(self.configCacheDir)

        encodedURL = base64.urlsafe_b64encode(configURL)
        configFileName = self.configCacheDir + "/" + encodedURL + ".py"

        if os.path.exists(configFileName):
            return configFileName

        urlFileHandle = urllib.urlopen(configURL)
        cacheFileHandle = open(configFileName, "w")
        cacheFileHandle.write(urlFileHandle.read())
        cacheFileHandle.close()
        urlFileHandle.close()

        return configFileName

    def getProcessingStyle(self, streamID):
        """
        _getProcessingStyle_

        Retrieve the processing style for the given stream ID.  Bulk will be
        returned if no processing style is defined.
        """
        if self.processingStyles == None:
            self.t0astLoadProcessingStyles()

        return self.processingStyles.get(streamID, "Bulk")
        
    def isDatasetExpress(self, datasetID):
        """
        _isDatasetExpress_
        
        Check if datasetID is part of a stream that is
        express processed
        """
        streamID = self.getStreamIDfromDatasetID(datasetID)

        if self.getProcessingStyle(streamID) == "Express":
            return True
        else:
            return False
    
    def isDatasetPathExpress(self, datasetPathID):
        """
        _isDatasetPathExpress_
        
        Check if datasetPathID is part of a stream that is
        express processed
        """
        datasetIDs = ListDatasets.listDatasetIDsFromID(self.t0astDBConn,
                                                       datasetPathID)
        
        return self.isDatasetExpress(datasetIDs["PRIMARY"])
    
            
    def getStreamIDfromDatasetID(self, datasetID):
        """
        _getStreamIDfromDatasetID_

        Return id of stream containing datasetID
        """
        if not self.t0astLoadStreamToDatasets():
            return None

        datasetName = self.getPrimaryDatasetName(datasetID)

        if datasetName == None:
            msg = "RunConfig fatal error : No datasetID %s in run %s\n" % (datasetID, self.runNumber)
            raise RuntimeError, msg

        if datasetName.startswith("Stream"):
            streamID = self.getStreamID(datasetName.lstrip("Stream"))
            if streamID != None:
                return streamID

        for streamID,datasets in self.streamToDatasets.items():
            if datasetID in datasets:
                return streamID

        msg = "RunConfig fatal error : This should never happen, bailing out!"
        raise RuntimeError, msg

    def getStreamIDfromDatasetPathID(self, datasetPathID):
        """
        _getStreamIDfromDatasetID_

        Return id of stream containing datasetID
        """
        datasetIDs = ListDatasets.listDatasetIDsFromID(self.t0astDBConn,
                                                       datasetPathID)
        return self.getStreamIDfromDatasetID(datasetIDs["PRIMARY"])

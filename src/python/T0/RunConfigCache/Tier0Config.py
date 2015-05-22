#!/usr/bin/env python
"""
_Tier0Config_

Holds the Tier0 configuration and provides methods for manipulating it.  The
Tier0 configuration has the following form:

Tier0Configuration - Global configuration object
| | |
| | |--> Global - Configuration parameters that do not belong to a particular
| |       |       stream or dataset and can be applied to an entire run.
| |       |
| |       |--> Version - The CVS revision of the config    
| |       |--> AcquisitionEra - The acquisition era for the run
| |       |--> RepackVersionMappings - Dictionary where the key is the framework
| |       |                            version used to take the run and the value
| |       |                            is the framework version that should be
| |       |                            used to repack the run.
| |       |--> ExpressVersionMappings - Dictionary where the key is the framework
| |       |                             version used to take the run and the value
| |       |                             is the framework version that should be
| |       |                             used to express process the run.
| |       |--> PhEDExSubscriptions - Dictionary of PhEDEx subscriptions where the
| |                                  primary dataset id is the key and the storage
| |                                  node name is the value.
| |
| |--> Streams - Configuration parameters that belong to a particular stream.
|       | |
|       | |--> Default - Configuration section to hold the default stream
|       |       |        configuration.
|       |       |
|       |       |--> ProcessingStyle - The processing style for the default
|       |                              stream.
|       |      
|       |--> STREAMNAME - Configuration section for a stream.
|             |
|             |--> ProcessingStyle - The processing style for the stream.
|             |
|             |--> CMSSWVersion - Framework version to use for express processing.
|             |
|             |--> ProcessingConfigURL - URL to the processing configuration if
|             |                          this stream is to use express processing. 
|             |
|             |--> DataTiers          = List of data tiers (for express processing)
|             |
|             |--> AlcaMergeConfigURL - URL to the Alca merge configuration
|             |                         (used for express style ALCARECO merging)
|             |
|             |--> SplitInProcessing - If express processing is enabled for this
|             |                        stream this will determine whether or not
|             |                        data is split into datasets during the
|             |                        processing step or the merge packing step.
|             |
|             |--> ProcessingVersion - The processing version for the express
|             |                        stream.
|             |--> DATASETNAME - Configuration object for a dataset.
|
|--> Datasets
      | |
      | |--> Default - Configuration section to hold the default dataset config.
      |                This will have the same structure as the dataset config
      |                listed below.
      |
      |--> DATASETNAME
            |--> Name - Name of the dataset.
            |--> Scenario - String that describes the processing scenario for this
            |               dataset.
            |--> CustodialNode - The custodial PhEDEx storage node for this dataset
            |--> ArchivalNode - The archival PhEDEx node, always CERN.
            |--> CustodialPriority - The priority of the custodial subscription
            |--> CustodialAutoApprove - Determine whether or not the custodial
            |                           subscription will be auto approved.
            |
            |--> Repack - Configuration section to hold settings related to 
            |     |       repacking.
            |     | 
            |     |--> ProcessingVersion - Processing version for repacking.
            |
            |--> Reco - Configuration section to hold settings related to prompt
            |     |     reconstruction.
            |     |
            |     |--> DoReco - Either True or False.  Determines whether prompt
            |     |             reconstruction is preformed on this dataset.
            |     |--> GlobalTag - The global tag that will be used to prompt
            |     |                reconstruction.  Only used if DoReco is true.
            |     |--> CMSSWVersion - Framework version to be used for prompt
            |     |                   reconstruction.  This only needs to be filled
            |     |                   in if DoReco is True and will default to 
            |     |                   "Undefined" if not set.
            |     |--> ConfigURL - URL of the framework configuration file.  If not set
            |     |                 the configuration will be pulled from the framework.
            |     |--> ProcessingVersion - Processing version for reconstruction.
            |  
            |--> Alca - Configuration section to hold settings related to alca 
            |     |     production.
            |     |
            |     |--> DoAlca - Either True or False.  Determines whether alca production
            |     |             is preformed on this dataset.
            |     |--> CMSSWVersion - Framework version to be used for alca production.  
            |     |                   This only needs to be filled in if DoAlca is True
            |     |                   and will default to 'Undefined' if not set.
            |     |--> ConfigURL - URL of the framework configuration file.  If not set 
            |     |                the configuration will be pulled from the framework.
            |     |--> ProcessingVersion - Processing version for alca.
            |
            |--> WMBSPublish - Configuration section to hold settings related to WMBS
            |     |            publication
            |     |
            |     |--> DoWMBSPublish - Either True or False.  Determines whether publication
            |     |                    is preformed on this dataset.
            |     |--> DataTiersTo - Which data tiers get published for this primary dataset
            |                        and to where
            |
            |--> DQM - Configuration section to hold settings related to dqm production.
            |     |
            |     |--> DoDQM - Either True or False.  Determines whether DQM production is
            |     |            preformed on this dataset.
            |     |--> CMSSWVersion - Framework version to be used for DQM production.
            |     |                   This only needs to be filled in if DoDQM is True and
            |     |                   will default to 'Undefined' in not set.
            |     |--> ConfigURL - URL of the framework configuration file.  If not set the
            |     |                 configuration will be pulled from the framework.
            |     |--> ProcessingVersion - Processing version for DQM.
            |
            |--> Tier1Skims - List of configuration section objects to hold Tier1 skims for
                  |           this dataset.
                  |
                  |--> DataTier - The tier of the input data.
                  |--> PrimaryDataset - The primary dataset of the input data.
                  |--> CMSSWVersion - The framework version to use with the skim.
                  |--> SkimName - The name of the skim.  Used for generating more descriptive job names.
                  |--> ConfigURL - A URL to the framework config for the config.
                  |--> ProcessingVersion - The processing version of the skim.
                  |--> TwoFileRead - Bool that determines if this is a two file read skim.
"""

__revision__ = "$Id: Tier0Config.py,v 1.22 2009/07/18 15:23:12 hufnagel Exp $"
__version__ = "$Revision: 1.22 $"

import logging
import copy

from WMCore.Configuration import Configuration
from WMCore.Configuration import ConfigSection

def createTier0Config():
    """
    _createTier0Config_

    Create a configuration object to hold the Tier0 configuration.  Currently,
    the configuration has two sections: Streams and Global.
    """
    tier0Config = Configuration()
    tier0Config.section_("Streams")
    tier0Config.section_("Datasets")    
    tier0Config.section_("Global")

    tier0Config.Global.RepackVersionMappings = {}
    tier0Config.Global.ExpressVersionMappings = {}

    return tier0Config

def retrieveStreamConfig(config, streamName):
    """
    _retrieveStreamConfig_
    
    Lookup the configuration for the given stream.  If the configuration for a
    particular stream is not explicitly defined, return the default configuration.
    """
    streamConfig =  getattr(config.Streams, streamName, None)
    if streamConfig == None:
        streamConfig = config.Streams.section_(streamName)
        streamConfig.ProcessingStyle = "Bulk"

    return streamConfig

def retrieveDatasetConfig(config, datasetName):
    """
    _retrieveDatasetConfig_
    
    Lookup the configuration for the given dataset.  If the configuration for a
    particular dataset is not defined return the default configuration.
    """
    datasetConfig = getattr(config.Datasets, datasetName, None)
    if datasetConfig == None:
        datasetConfig = copy.deepcopy(config.Datasets.Default)
        datasetConfig.Name = datasetName
        
    return datasetConfig
        
def addDataset(config, datasetName, **settings):
    """
    _addDataset_

    Add a dataset to the configuration using settings from the Default dataset
    to fill in any parameter not explicitly defined here.  By default only
    repacking is turned on.

    The following keys may be passed in to alter the default settings:
      scenario - The scenario for this dataset.
      global_tag - The global tag to use for reco.
      do_repack, do_dqm, do_alca, do_reco - Disable/enable repacking, dqm,
        alca and reco.  Repacking is on by default, all other are off.
      repack_version, reco_version, alca_version, dqm_version - Framework
        versions to use for repacking, reco, alca and dqm.  No defaults are
        specified.
      default_proc_ver - Processing version that will be applied to all
        processing steps.  This will be applied first and then the specific
        processing versions will be applied.
      repack_proc_ver, reco_proc_ver, alca_proc_ver, dqm_proc_ver - Processing
        versions to use for the various data processing steps.  No defaults are
        specified.
      reco_configuration, alca_configuration, dqm_configuration - Configurations
        to use for reco, alca and dqm.  No defaults are specified.
      custodial_node - The PhEDEx custodial node for this dataset.
      archival_node - The PhEDEx archival node for this dataset, always CERN.
      custodial_priority - The priority of the custodial PhEDEx subscription,
        defaults to high.
      custodial_auto_approve - Whether or not the custodial subscription is auto
        approved.  Defaults to false.
    """
    datasetInstance = getattr(config.Datasets, datasetName, None)
    if datasetInstance == None:
        defaultInstance = getattr(config.Datasets, "Default", None)

        if defaultInstance != None:
            datasetInstance = copy.deepcopy(defaultInstance)
            datasetInstance._internal_name = datasetName
            setattr(config.Datasets, datasetName, datasetInstance)
        else:
            datasetInstance = config.Datasets.section_(datasetName)

    datasetInstance.Name = datasetName
    datasetInstance.Scenario = settings.get("scenario", "collision")

    datasetInstance.section_("Repack")
    datasetInstance.section_("Reco")
    datasetInstance.section_("Alca")
    datasetInstance.section_("WMBSPublish")
    datasetInstance.section_("DQM")

    default_proc_ver = settings.get("default_proc_ver", None)

    datasetInstance.Repack.ProcessingVersion = settings.get("repack_proc_ver",
                                                            default_proc_ver)
    
    datasetInstance.Reco.DoReco = settings.get("do_reco", False)
    datasetInstance.Reco.GlobalTag = settings.get("global_tag", None)
    datasetInstance.Reco.CMSSWVersion = settings.get("reco_version", "Undefined")
    datasetInstance.Reco.ConfigURL = settings.get("reco_configuration",
                                                      None)
    datasetInstance.Reco.ProcessingVersion = settings.get("reco_proc_ver",
                                                          default_proc_ver)

    datasetInstance.Alca.DoAlca = settings.get("do_alca", False)
    datasetInstance.Alca.CMSSWVersion = settings.get("alca_version", "Undefined")
    datasetInstance.Alca.ConfigURL = settings.get("alca_configuration",
                                                      None)
    datasetInstance.Alca.ProcessingVersion = settings.get("alca_proc_ver",
                                                          default_proc_ver)

    datasetInstance.WMBSPublish.DoWMBSPublish = settings.get("do_wmbs_publish", False)
    datasetInstance.WMBSPublish.DataTiersTo = settings.get("wmbs_publish_data_tiers_to", [] )

    datasetInstance.DQM.DoDQM = settings.get("do_dqm", False)
    datasetInstance.DQM.CMSSWVersion = settings.get("dqm_version", "Undefined")
    datasetInstance.DQM.ConfigURL = settings.get("dqm_configuration", None)
    datasetInstance.DQM.ProcessingVersion = settings.get("dqm_proc_ver",
                                                         default_proc_ver)

    datasetInstance.CustodialNode = settings.get("custodial_node", None)
    datasetInstance.ArchivalNode = settings.get("archival_node", None)
    datasetInstance.CustodialPriority = settings.get("custodial_priority",
                                                     "high")
    datasetInstance.CustodialAutoApprove = settings.get("custodial_auto_approve",
                                                        False)

    datasetInstance.Tier1Skims = []    
    return

def setAcquisitionEra(config, acquisitionEra):
    """
    _setAcquisitionEra_

    Set the acquisition era in the configuration.
    """
    config.Global.AcquisitionEra = acquisitionEra
    return

def setConfigVersion(config, version):
    """
    _setConfigVersion_

    Set the version of the config.  This will more than likely be the CVS
    revision of the configuration.
    """
    config.Global.Version = version
    return

def addExpressConfig(config, streamName, **options):
    """
    _addExpressConfig_

    Add an express configuration to a given stream.  The stream must have a
    processing style of 'Express' for this to have any meaning.
    """
    proc_config = options.get("proc_config", None)
    if proc_config == None:
        msg = "Tier0Config.addExpressConfig : no proc_config defined for stream %s" % streamName
        raise RuntimeError, msg

    data_tiers = options.get("data_tiers", [])
    if type(data_tiers) != list or len(data_tiers) == 0:
        msg = "Tier0Config.addExpressConfig : data_tiers needs to be list with at least one tier"
        raise RuntimeError, msg

    alcamerge_config = None
    if "ALCARECO" in data_tiers:
        alcamerge_config = options.get("alcamerge_config", None)
        if alcamerge_config == None:
            msg = "Tier0Config.addExpressConfig : alcamerge_config needed for ALCARECO"
            raise RuntimeError, msg

    global_tag = options.get("global_tag", None)

    streamConfig = config.Streams.section_(streamName)
    streamConfig.ProcessingStyle = "Express"

    streamConfig.ProcessingConfigURL = proc_config
    streamConfig.DataTiers = data_tiers
    streamConfig.AlcaMergeConfigURL = alcamerge_config
    streamConfig.GlobalTag = global_tag

    streamConfig.SplitInProcessing = options.get("splitInProcessing", False)
    streamConfig.ProcessingVersion = options.get("proc_ver", "v1")

    return

def setRepackVersionMapping(config, runVersion, repackVersion):
    """
    _setRepackVersionMapping_

    Associate a framework version that was used to take a run
    with the version that should be used to repack it
    """
    config.Global.RepackVersionMappings[runVersion] = repackVersion
    return

def setExpressVersionMapping(config, runVersion, expressVersion):
    """
    _setExpressVersionMapping_

    Associate a framework version that was used to take a run
    with the version that should be used to express process it
    """
    config.Global.ExpressVersionMappings[runVersion] = expressVersion
    return

def addTier1Skim(config, skimName, dataTier, primaryDataset, cmsswVersion,
                 processingVersion, configURL, twoFileRead = False):
    """
    _addTier1Skim_

    Add the configuration of a skim that is to be run over a particular primary
    dataset and data tier at a particular site to the Tier0 configuration.  The
    skims will be launched as blocks are transfered to the site.  The site name
    must correspond to the site name in the ProdAgent JobQueue.
    """
    datasetInstance = config.Datasets.section_(primaryDataset)    

    skimConfig = ConfigSection(name = "SomeTier1Skim")
    skimConfig.PrimaryDataset = primaryDataset
    skimConfig.DataTier = dataTier
    skimConfig.SkimName = skimName
    skimConfig.CMSSWVersion = cmsswVersion
    skimConfig.ConfigURL = configURL
    skimConfig.ProcessingVersion = processingVersion
    skimConfig.TwoFileRead = twoFileRead

    datasetInstance.Tier1Skims.append(skimConfig)
    return

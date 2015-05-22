#!/usr/bin/env python
"""
_OfflineConfiguration_

Processing configuration for the Tier0.
"""

__revision__ = "$Id: DevOfflineConfiguration.py,v 1.13 2009/06/29 18:40:52 hufnagel Exp $"
__version__ = "$Revision: 1.13 $"

from T0.RunConfigCache.Tier0Config import addDataset
from T0.RunConfigCache.Tier0Config import addTier1Skim
from T0.RunConfigCache.Tier0Config import createTier0Config
from T0.RunConfigCache.Tier0Config import setAcquisitionEra
from T0.RunConfigCache.Tier0Config import setConfigVersion
from T0.RunConfigCache.Tier0Config import addExpressConfig
from T0.RunConfigCache.Tier0Config import setRepackVersionMapping
from T0.RunConfigCache.Tier0Config import setExpressVersionMapping

# Create the Tier0 configuration object
tier0Config = createTier0Config()

# Set global parameters like the acquisition era and the version of
# the configuration.
setAcquisitionEra(tier0Config, "Comissioning08")
setConfigVersion(tier0Config, __version__)

# Setup some useful defaults: processing version, reco framework version,
# global tag.
defaultProcVersion = "v1"
defaultRecoVersion = "CMSSW_2_1_10"
defaultAlcaVersion = "CMSSW_2_2_5"
defaultGlobalTag = "CRUZET4_V6P::All"

# Create a dictionary that associates a reco configuration with a scenario.
# The configuration must be specified as a url.
recoConfig = {}
recoConfig["collisions1"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_cfg.py?revision=1.25"
recoConfig["collisions2"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_cfg.py?revision=1.24"
recoConfig["collisions3"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_cfg.py?revision=1.23"

# Create a dictionary that associates skim names to config urls.
skimConfig = {}
skimConfig["SuperPointing"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/SuperPointing_cfg.py?revision=1.12"
skimConfig["TrackerPointing"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/TrackerPointing_cfg.py?revision=1.9"
skimConfig["HcalHPDFilter"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/HcalHPDFilter_cfg.py?revision=1.2"

# Create a dictionary that associates express processing config urls to names.
expressProcConfig = {}
expressProcConfig["default"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/DPGAnalysis/Skims/python/HcalHPDFilter_cfg.py?revision=1.2"

# Create a dictionary that associates alca processing config urls to names.
alcaConfig = {}
alcaConfig["cosmics"] = "http://cern.ch/gowdy/AlCaReco_step3_Cosmics_cfg.py"

# actual express configuration
# Create a dictionary that associates express processing config urls to names.
expressProcConfig = {}
#expressProcConfig["default"] = "/data/hufnagel/parepack/configuration/testExpressProc_cfg.py"
#expressProcConfig["default"] = "/data/hufnagel/parepack/configuration/raw2digi_reco_express_cfg.py"
expressProcConfig["default"] = "/data/hufnagel/parepack/configuration/raw2digi_reco_alcaCombined_express_cfg.py"

# Create a dictionary that associated express merge packing config urls to names.
expressAlcaMergeConfig = {}
#expressMergePackConfig["default"] = "/data/hufnagel/parepack/configuration/testExpressProc_cfg.py"
expressAlcaMergeConfig["default"] = "/data/hufnagel/parepack/configuration/alCaRecoSplitting_express_cfg.py"

addExpressConfig(tier0Config, "Express",
                 proc_config = expressProcConfig["default"],
                 data_tiers = [ "RAW", "RECO", "ALCARECO" ],
                 alcamerge_config = expressAlcaMergeConfig["default"],
                 splitInProcessing = False,
                 proc_ver = "v1")

addExpressConfig(tier0Config, "HLTMON",
                 proc_config = expressProcConfig["default"],
                 data_tiers = [ "FEVTHLTALL" ],
                 splitInProcessing = True,
                 proc_ver = "v1")

# Create the default configuration.  Repacking is enabled with HLTDebug
# splitting and everything else is turned off.  The default processing style
# is also set to "Bulk".
addDataset(tier0Config, "Default",
           default_proc_ver = defaultProcVersion, hltdebug = False,
           do_reco = False, do_alca = False, do_dqm = False,
           reco_version = defaultRecoVersion,
           custodial_node = "TX_Test1_Buffer",
           archival_node = "TX_Test2_Buffer",
           priority="normal",
           requestOnly="y")


# Actual configuration for datasets.  The Cosmics, HcalHPDNoise and MinimumBias
# datasets will be reconstructed, with different framework versions, different
# configs, different scenarios and different processing versions.  The remaining
# datasets (Monitor, RandomTriggers and TestEnables) will only be repacked.
addDataset(tier0Config, "Cosmics",
           default_proc_ver = defaultProcVersion, 
           do_reco = True, global_tag = defaultGlobalTag + "a",
           reco_configuration = recoConfig["collisions1"],
           reco_version = "CMSSW_2_1_8", reco_proc_ver = "v1",
           do_alca = True,
           alca_configuration = alcaConfig["cosmics"],
           alca_version = defaultAlcaVersion,           
           custodial_node = "TX_Test1_Buffer",
           archival_node = "TX_Test2_Buffer",
           custodial_priority = "normal",
           custodial_auto_approve = True)

addDataset(tier0Config, "HcalHPDNoise",
           default_proc_ver = defaultProcVersion, 
           do_reco = True, global_tag = defaultGlobalTag + "b",
           reco_configuration = recoConfig["collisions2"],
           reco_version = "CMSSW_2_1_9", reco_proc_ver = "v2",
           do_alca = True,
           alca_configuration = alcaConfig["cosmics"],
           alca_version = defaultAlcaVersion,           
           custodial_node = "TX_Test1_Buffer",
           archival_node = "TX_Test2_Buffer",
           custodial_priority = "normal",
           custodial_auto_approve = False)
           
addDataset(tier0Config, "MinimumBias",
           default_proc_ver = defaultProcVersion, 
           do_reco = True, global_tag = defaultGlobalTag + "c",
           reco_configuration = recoConfig["collisions3"],
           reco_version = "CMSSW_2_1_10", reco_proc_ver = "v3",
           do_alca = True,
           alca_configuration = alcaConfig["cosmics"],
           alca_version = defaultAlcaVersion,           
           custodial_node = "TX_Test1_Buffer",
           archival_node = "TX_Test2_Buffer",
           custodial_priority = "normal",
           custodial_auto_approve = False)

addDataset(tier0Config, "TestEnables",
           default_proc_ver = defaultProcVersion, 
           do_reco = False,
           custodial_node = "TX_Test1_Buffer",
           archival_node = "TX_Test2_Buffer",
           priority="normal",
           requestOnly="y")
           
# Add in the Tier1 skims.  Each dataset that is skimmed needs to already have
## a configuration.
#addTier1Skim(tier0Config, "Skim1", "A", "RECO", "Cosmics", "CMSSW_2_1_10", "v1",
#             skimConfig["SuperPointing"], "fnal.gov")
#addTier1Skim(tier0Config, "Skim2", "A", "RECO", "Cosmics", "CMSSW_2_1_9", "v2",
#             skimConfig["TrackerPointing"], "fnal.gov")
#addTier1Skim(tier0Config, "Skim3", "A", "RECO", "MinimumBias", "CMSSW_2_1_8", "v3",
#             skimConfig["HcalHPDFilter"], "in2p3.something")


# Setup the mappings between the framework version used to take a run and the
# version that should be used to repack it.
setRepackVersionMapping(tier0Config, "CMSSW_2_0_4", "CMSSW_2_0_12")
setRepackVersionMapping(tier0Config, "CMSSW_2_0_8", "CMSSW_2_0_12")
setRepackVersionMapping(tier0Config, "CMSSW_2_0_10", "CMSSW_2_0_12")
setRepackVersionMapping(tier0Config, "CMSSW_2_1_1", "CMSSW_2_1_8")
setRepackVersionMapping(tier0Config, "CMSSW_2_1_4", "CMSSW_2_1_8")

setExpressVersionMapping(tier0Config, "CMSSW_2_0_4", "CMSSW_2_0_12")
setExpressVersionMapping(tier0Config, "CMSSW_2_0_8", "CMSSW_2_0_12")
setExpressVersionMapping(tier0Config, "CMSSW_2_0_10", "CMSSW_2_0_12")
setExpressVersionMapping(tier0Config, "CMSSW_2_1_1", "CMSSW_2_1_8")
setExpressVersionMapping(tier0Config, "CMSSW_2_1_4", "CMSSW_2_1_8")

if __name__ == '__main__':
    print tier0Config

#!/usr/bin/env python
"""
_OfflineConfiguration_

Processing configuration for the Tier0.
"""

__revision__ = "$Id: OfflineConfiguration.py,v 1.14 2009/07/18 15:23:46 hufnagel Exp $"
__version__ = "$Revision: 1.14 $"

from T0.RunConfigCache.Tier0Config import addDataset
from T0.RunConfigCache.Tier0Config import createTier0Config
from T0.RunConfigCache.Tier0Config import setAcquisitionEra
from T0.RunConfigCache.Tier0Config import setConfigVersion
from T0.RunConfigCache.Tier0Config import setRepackVersionMapping
from T0.RunConfigCache.Tier0Config import setExpressVersionMapping
from T0.RunConfigCache.Tier0Config import addExpressConfig

# Create the Tier0 configuration object
tier0Config = createTier0Config()

# Set global parameters like the acquisition era and the version of
# the configuration.
setAcquisitionEra(tier0Config, "ReRepack_AllCruzet_1")
#setAcquisitionEra(tier0Config, "ReRepack_BeamCommissioning08_10")
setConfigVersion(tier0Config, __version__)


# Setup some useful defaults: processing version, reco framework version,
# global tag.
defaultProcVersion = "v1"
defaultRecoVersion = "CMSSW_2_2_13"
defaultAlcaVersion = "CMSSW_2_2_13"
#global tag for the AllCruzet replays
defaultGlobalTag = "CRUZETALL_V6::All"
#global tag for the BeamCommissioning08 replays
#defaultGlobalTag = "BEAMSPLASH_V1::All"
#global tag for the CR3TEW35 replays
#defaultGlobalTag = "EW35_V1::All


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

# Create a dictionary that associates a reco configuration with a scenario.
# The configuration must be specified as a url.
recoConfig = {}

# for CRUZET reprocessing
recoConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/recoT0DQM_EvContent_cfg.py?revision=1.44"


addExpressConfig(tier0Config, "Express",
                 proc_config = expressProcConfig["default"],
                 data_tiers = [ "RAW", "RECO", "ALCARECO" ],
                 alcamerge_config = expressAlcaMergeConfig["default"],
                 global_tag = "CRAFT_ALL_V12::All",
                 splitInProcessing = False,
                 proc_ver = "v1")

addExpressConfig(tier0Config, "HLTMON",
                 proc_config = expressProcConfig["default"],
                 data_tiers = [ "FEVTHLTALL" ],
                 splitInProcessing = True,
                 proc_ver = "v1")


# Create a dictionary that associates a alca configuration with a scenario.
# The configuration must be specified as a url.
alcaConfig = {}
alcaConfig["cosmics"] = "http://cern.ch/gowdy/AlCaReco_step3_Cosmics_cfg.py"
#alcaConfig["cosmics"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/AlCaReco_step3_Cosmics_cfg.py?revision=1.1"

addDataset(tier0Config, "Default",
           default_proc_ver = defaultProcVersion, hltdebug = False,
           do_reco = False, do_alca = False, do_dqm = False)

# Actual configuration for datasets.  The Calo, Cosmics and MinimumBias
# datasets will be reconstructed.
addDataset(tier0Config, "Cosmics",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion,
           do_alca = True,
           alca_configuration = alcaConfig["cosmics"],
           alca_version = defaultAlcaVersion )
addDataset(tier0Config, "MinimumBias",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion,
           do_alca = True,
           alca_configuration = alcaConfig["cosmics"],
           alca_version = defaultAlcaVersion )
addDataset(tier0Config, "BeamHalo",
           default_proc_ver = defaultProcVersion, scenario = "cosmics",
           do_reco = True, global_tag = defaultGlobalTag,
           reco_configuration = recoConfig["cosmics"],
           reco_version = defaultRecoVersion,
           do_alca = True,
           alca_configuration = alcaConfig["cosmics"],
           alca_version = defaultAlcaVersion )
#addDataset(tier0Config, "BarrelMuon",
#           default_proc_ver = defaultProcVersion, scenario = "cosmics",
#           do_reco = True, global_tag = defaultGlobalTag,
#           reco_configuration = recoConfig["cosmics"],
#           reco_version = defaultRecoVersion)
#addDataset(tier0Config, "EndcapsMuon",
#           default_proc_ver = defaultProcVersion, scenario = "cosmics",
#           do_reco = True, global_tag = defaultGlobalTag,
#           reco_configuration = recoConfig["cosmics"],
#           reco_version = defaultRecoVersion)

# Setup the mappings between the framework version used to take a run
# and the version that should be used to repack it.
setRepackVersionMapping(tier0Config, "CMSSW_2_0_4", "CMSSW_2_0_12")
setRepackVersionMapping(tier0Config, "CMSSW_2_0_8", "CMSSW_2_0_12")
setRepackVersionMapping(tier0Config, "CMSSW_2_0_10", "CMSSW_2_0_12")
setRepackVersionMapping(tier0Config, "CMSSW_2_1_1", "CMSSW_2_1_8")
setRepackVersionMapping(tier0Config, "CMSSW_2_1_4", "CMSSW_2_1_8")
setRepackVersionMapping(tier0Config, "CMSSW_2_1_9", "CMSSW_2_1_19")
setRepackVersionMapping(tier0Config, "CMSSW_2_2_10", "CMSSW_2_2_11_offpatch1")

# Setup the mappings between the framework version used to take a run
# and the version that should be used to express process it
setExpressVersionMapping(tier0Config, "CMSSW_2_0_4", "CMSSW_2_0_12")
setExpressVersionMapping(tier0Config, "CMSSW_2_0_8", "CMSSW_2_0_12")
setExpressVersionMapping(tier0Config, "CMSSW_2_0_10", "CMSSW_2_0_12")
setExpressVersionMapping(tier0Config, "CMSSW_2_1_1", "CMSSW_2_1_8")
setExpressVersionMapping(tier0Config, "CMSSW_2_1_4", "CMSSW_2_1_8")
setExpressVersionMapping(tier0Config, "CMSSW_2_1_9", "CMSSW_2_1_19")
setExpressVersionMapping(tier0Config, "CMSSW_2_2_10", "CMSSW_2_2_13")

if __name__ == '__main__':
    print tier0Config

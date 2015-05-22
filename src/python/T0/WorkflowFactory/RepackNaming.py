#!/usr/bin/env python
"""
_RepackNaming_

Provides method to build LFN bases (merged and unmerged)
for repacked files

Method is kind of generic though and would work the same
way for RECO and AOD, so maybe we generalize it later

"""

def getLFN(outputModuleRef, run, **options):
    """
    _default_

    Build a default name structure based on the parameters passed.

    """

    if outputModuleRef.get("acquisitionEra") == None:
        msg = "Acquistion Era is not specified!!!\n"
        raise RuntimeError, msg

##    lfn = "/T0/hufnagel/repacktest/store/"
    lfn = "/store/"
    if options.get("Unmerged", False):
        lfn += "temp/"
    lfn += "data/"
    lfn += "%s/" % outputModuleRef.get("acquisitionEra")
    lfn += "%s/" % outputModuleRef.get("primaryDataset")
    lfn += "%s/" % outputModuleRef.get("dataTier")
##    if options.get("Unmerged", False):
##        lfn += "%s/" % outputModuleRef.get("processedDataset")
##    else:
##        lfn += "%s/" % outputModuleRef.get("processedDataset").rstrip("-unmerged")
    lfn += "%s/" % outputModuleRef.get("processingVersion")
    runString = str(run).zfill(9)
    lfn += "%s/%s/%s" % (runString[0:3],
                         runString[3:6],
                         runString[6:9])
    lfn += "/"

    return lfn

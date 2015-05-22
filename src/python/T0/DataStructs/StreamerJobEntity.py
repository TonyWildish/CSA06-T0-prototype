#!/usr/bin/env python
"""
_StreamerJobEntity_

data structure to keep details of a job running on streamers

"""

__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: StreamerJobEntity.py,v 1.2 2009/02/20 16:35:31 hufnagel Exp $"


class StreamerJobEntity(dict):
    """
    _StreamerJobEntity_

    A dictionary based data structure containing details of a job that
    runs on streamer files (all files bookkeept in the streamer table).

    By design, this class only supports jobs that process files
    from the same run.

    It contains :

      jobID               unique id, have to get it from T0AST
      jobName             name of the job (includes id usually)
      streamerFiles       dict mapping of input streamer lfns to lumi section
      runNumber           run the input data belongs to
      lumiSections        lumi sections the input data belongs to
      streamerIDs         list of database ids for the input streamers
      datasetIDs          list of database ids for the output datasets

      activeOutputModules list of outputModule names (primary dataset names)
                          (used by the repacker runtime to prune output modules)
                          (should be removed at some point, ugly)

    """
    def __init__(self, **args):
        
        dict.__init__(self)

        # should come from T0AST and be the job_id from
        # the corresponding job_def table
        self.setdefault("jobID", None)

        # set when the job is created
        self.setdefault("jobName", None)

        # depends on what input the job is running on
        # is set when the job is being scheduled
        self.setdefault("streamerFiles", {})
        self.setdefault("runNumber", None)
        self.setdefault("lumiSections", [])
        self.setdefault("streamerIDs", [])
        self.setdefault("datasetIDs", [])
        self.setdefault("streamID", None)

        # depends on what output the job produces
        # is set when the job is being scheduled (if needed)
        self.setdefault("activeOutputModules", [])

        self.update(args)


    def addTriggerSegments(self, segments):
        """
        _addTriggerSegments_

        convinence method used by RepackerScheduler

        """
        for segment in segments:
            self['streamerIDs'].append(segment['StreamerID'])
            self['datasetIDs'].append(segment['DatasetID'])

        # remove duplicates
        self['streamerIDs'] = list(set(self['streamerIDs']))
        self['datasetIDs'] = list(set(self['datasetIDs']))


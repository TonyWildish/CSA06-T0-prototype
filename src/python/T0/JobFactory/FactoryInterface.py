#!/usr/bin/env python
"""
_FactoryInterface_

Interface class for JobSpec Factory classes, providing
common utils for all implementations

"""

from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec



class FactoryInterface:
    """
    _FactoryInterface_

    JobSpec Factory Interface defintion & common utils for
    all job spec factory generators

    """
    def __init__(self, workflowSpec):
        # or use isinstance(WorkflowSpec) if need to include sub classes
        if workflowSpec.__class__ is WorkflowSpec:
            self.workflow = workflowSpec
        else:
            self.workflow = WorkflowSpec()
            self.workflow.load(workflowSpec)

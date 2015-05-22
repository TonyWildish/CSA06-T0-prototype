#!/usr/bin/env python
"""
_TriggerPath_

Object representing a Trigger Path

"""

from IMProv.IMProvNode import IMProvNode


class TriggerPath(dict):
    """
    _TriggerPath_

    Trigger Path data container with SelectEvents PSet information
    
    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("Name", None)
        self.setdefault("SelectEvents", None)
        self.setdefault("EventCount", None)
        self.setdefault("ErrorCount", None)
        
        
    def save(self):
        """
        _save_

        Create IMProvNode repr of self

        """
        result = IMProvNode("TriggerPath", None, Name = self['Name'])
        for key, val in self.items():
            if key == "Name":
                continue
            if val == None:
                continue
            result.addNode(IMProvNode(key, None, Value = str(val)))
        return result


    def load(self, improvNode):
        """
        _load_

        Unpack values from improvNode into self

        """
        self['Name'] = improvNode.attrs.get('Name', None)
        for node in improvNode.children:
            key = node.name
            value = node.attrs.get("Value", None)
            if value == None:
                continue
            self[key] = value
        return
        

                           
        
        
        

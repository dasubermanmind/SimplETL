from abc import abstractclassmethod


"""
    This is the Base Class that all Transformers that will inherit from
    
    Extract functionality will be done done within the child class.
    Transform: Should be (will) be generic enough for all child classes but
    Each datum should be done within a normalize helper that each child class will implement themselves.
    Load: Should Also be generic 
"""


class Optimus:
    
    
    
    def __init__(self):
        pass

    # These are part of the contract. These must be implemented for each transformer
    @abstractclassmethod
    def normalize(self , data):
       raise NotImplementedError
    
    
    @abstractclassmethod
    def extract(self):
        raise NotImplementedError
    
    def Transform(self, data):
        pass
    
    
    def load(self, data):
        pass


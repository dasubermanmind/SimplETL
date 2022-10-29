
from typing import Any, List
import logging

class SimplETL:
    def __init__(self, project_name, environment, endpoint) -> None:
        self.project_name = project_name
        self.environment = environment
        self.endpoint = endpoint
        self.logger = logging.basicConfig(level=logging.DEBUG)
        
    
    data_to_transform = []
    data_to_load = []
    
    def _start(self)->None:
        print('Exctracting dataset')
        self.extract()
        
            
    # TODO: Need to investigate on whether or not 
    # I should always return a df
    def extract(self)-> List[Any]:
        pass
    
    
    def transform(self, data: List[Any])-> List[Any]:
        pass
    
    
    def load(self,data: List[Any])-> None:
        pass
    




from typing import Any, List


class AbstractSimplETL:
    def __init__(self, project_name, environment, endpoint) -> None:
        self.project_name = project_name
        self.environment = environment
        self.endpoint = endpoint
        
    
    def extract(self):
        pass
    
    
    def transform(self, data: List[Any]):
        pass
    
    
    def load(self,data: List[Any]):
        pass
    



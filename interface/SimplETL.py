
from typing import Any, Dict, Optional, Union
import logging
from rich.console import Console
import pandas as pd

from settings.general import COVID_DIR
from utilities.Utilities import Utilities

console = Console()

class SimplETL:
    def __init__(self, project_name, environment, endpoint) -> None:
        self.project_name = project_name
        self.environment = environment
        self.endpoint = endpoint
        self.logger = logging.basicConfig(level=logging.DEBUG)
        
    
    data_to_transform = []
    data_to_load = []
    
    def _start(self, csv: ...)->None:
        """
            The main entry point of the ETL. Within this phase we first setup
            all dependancies, authentication & any misc tasks we need to do before
            the pipeline begins
            
            Returns None
        """
        console.print('Starting Preprocess Phase')
        # Do some preprocessing then call an driver that executes the 
        # main ETL
        data = pd.read_csv(csv)
        
        
        self.execute(data)
        
        
    # TODO: Need to investigate on whether or not 
    # I should always return a df
    def extract(self, data: Optional[Any])-> Union[Any, None]:
        if not data:
            return None
        self.data_to_transform.append(data)
        
        return self.data_to_transform
    
    
    def transform(self, data)-> Union[Any, None]:
        
        if len(self.data_to_transform) <=0:
            self.logger.info('Failed to extract properly')
           
        return None
        

    def execute(self,data):
        print('works')
        while True:
            try:
                # extract
                ret = self.extract(data)
                # transform
                transfom_val = self.transform(ret)
                # load
                self.load(transfom_val)
                if len(self.data_to_load) <= 0:
                    break
            except StopIteration:
                break
    
    
    def load(self, data)-> Dict[str,Any]:
        if not self.data_to_load:
            self.logger.info('Failed to Transform')
    



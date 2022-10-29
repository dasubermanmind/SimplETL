
from typing import Any, List
import logging

from pandas import read_csv
import pandas as pd

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
        columns = ['covid', 'infection_rate']
        # setup all the files in data folder
        # df = pd.DataFrame()
        # TODO: Research other operations I can do with read_csv
        df = pd.read_csv('filepath', header=columns)
        return df
    
    
    def transform(self, data: List[Any])-> List[Any]:
        pass
    
    
    def load(self,data: List[Any])-> None:
        pass
    



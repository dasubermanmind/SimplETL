
from typing import Any, Dict, List
import logging

from pandas import read_csv
import pandas as pd
#from kaggle.api.kaggle_api_extended import KaggleAPI

class SimplETL:
    def __init__(self, project_name, environment, endpoint) -> None:
        self.project_name = project_name
        self.environment = environment
        self.endpoint = endpoint
        self.logger = logging.basicConfig(level=logging.DEBUG)
        
    
    data_to_transform = []
    data_to_load = []
    
    def _start(self)->Dict[str,Any]:
        """
            The main entry point of the ETL. Within this phase we first setup
            all dependancies, authentication & any misc tasks we need to do before
            the pipeline begins
            
            Returns None
        """
        #api = KaggleApi()
        #api.authenticate()
        print('Starting Preprocess Phase')
        while True:
            try:
                # extract
                self.extract()
                # transform
                self.transform(self.data_to_transform)
                # load
                self.load(self.data_to_load)
                if len(self.data_to_load) <= 0:
                    break
            except StopIteration:
                break
        
        
            
    # TODO: Need to investigate on whether or not 
    # I should always return a df
    def extract(self)-> List[Any]:
        #kaggle datasets download -d reikagileletsoalo/covid-analysis
        columns = ['covid', 'infection_rate']
        # setup all the files in data folder
        # df = pd.DataFrame()
        # TODO: Research other operations I can do with read_csv
        df = pd.read_csv('data/covid/*.csv', header=columns)
        self.data_to_transform.append(df)
        return self.data_to_transform
    
    
    def transform(self, data)-> Dict[str,Any]:
        if len(self.data_to_transform) <=0:
            self.logger.info('Failed to extract properly')
           
        # def get_meta_data(self,self.data_to_transform):
        #      pass
        # determine which transform to apply
        structure: Dict[str,Any] = {
            'META_DATA': {},
            'MAIN_DATA': {},
            'CONDA_DATA': {},
        }
        #TODO: Apply intermediate transform and make sure user wants to 
        # apply to the entire data set
        
        return structure
        
    
    
    def load(self, data)-> Dict[str,Any]:
        if not self.data_to_load:
            self.logger.info('Failed to Transform')
    



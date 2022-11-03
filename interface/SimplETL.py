
from typing import Any, Dict, Optional
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
    def extract(self, data: Optional[Any])-> Any:
        if data:
            # use the data structure
            console.print('Inside data')
            Utilities.write_to_csv(data, 'covid.csv')
            return data
        
        else:
            columns = ['covid', 'infection_rate']
            # setup all the files in data folder
            # df = pd.DataFrame()
            # TODO: Research other operations I can do with read_csv
            df = pd.read_csv(COVID_DIR, header=columns)
            self.data_to_transform.append(df)
            return self.data_to_transform
    
    
    def transform(self, data)-> Any:
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
        

    def execute(self,data):
        print('works')
        while True:
            try:
                # extract
                self.extract(data)
                # transform
                self.transform(self.data_to_transform)
                # load
                self.load(self.data_to_load)
                if len(self.data_to_load) <= 0:
                    break
            except StopIteration:
                break
    
    
    def load(self, data)-> Dict[str,Any]:
        if not self.data_to_load:
            self.logger.info('Failed to Transform')
    



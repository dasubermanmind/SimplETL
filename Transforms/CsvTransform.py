
from typing import Any, Dict, List, Optional, Union
import logging
from rich.console import Console
import pandas as pd
from db.postgres import LoadPostgres

from settings.general import COVID_DIR
from utilities.Utilities import Utilities

console = Console()


class CsvTransform:
    def __init__(self, project_name, environment, endpoint) -> None:
        self.project_name = project_name
        self.environment = environment
        self.endpoint = endpoint
        self.logger = logging.basicConfig(level=logging.DEBUG)
        
    
    data_to_transform = []
    data_to_load = []
    
    def _start(self, csv: Optional[Any])->None:
        """
            The main entry point of the ETL. Within this phase we first setup
            all dependancies, authentication & any misc tasks we need to do before
            the pipeline begins
            
            Returns None
        """
        console.print('Starting Preprocess Phase')
        if csv:
            data: pd.DataFrame = pd.read_csv(csv, na_values='No data gathered')
        else:
            # parse out the csv
            pass
        
        self.execute(data)
        
    def extract(self, data: Optional[Any])-> Union[pd.DataFrame, List[Any]]:
        for _, row in data.iterrows():
            self.data_to_transform.append(row)
        
        return self.data_to_transform
    
    
    def transform(self, data: pd.DataFrame)-> Union[Any, None]:
        
        if len(data) <=0:
            self.logger.info('Failed to extract properly')
        
        print(type(data))
        # 
        
        return data
        

    def execute(self, data: pd.DataFrame):
        """
            This is the execution loop for the extraction
            
            Return None
        """
        while True:
            try:
                # extract
                extraction_data = self.extract(data)
                print(f'Extraction Data-->{extraction_data}')
                # transform
                transfom_data = self.transform(extraction_data)
                print(f'Transofrmed Data-->{transfom_data}')
                # load
                self.load(transfom_data)
                
                if len(self.data_to_load) <= 0:
                    break
            except StopIteration:
                break
    
    
    def load(self, data)-> Dict[str,Any]:
        """

        """
        connector = LoadPostgres()
        print(f'Connector--> {connector}')
        self.data_to_load.clear()
        
        # Load to a target endpoint....like postgres/neo4j
        print('Loading!')
    



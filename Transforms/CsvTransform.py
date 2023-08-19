from typing import Any, List, Optional
import pandas as pd
from rich.console import Console

from Transforms.Optimus import Optimus
from settings.general import DATA

console = Console()

class CsvTransform(Optimus):
   

    def __init__(self, project_name, env):        
        super().__init__(project_name, env)
        self.project_name = project_name
        self.environment = env
        print('INSIDE CSV TRANSFORM')


    def extract(self, data: ...) -> ...:
        """
            The main entry point of the ETL. Within this phase we first setup
            all dependancies & any misc tasks we need to do before
            the pipeline begins

            Returns datafram
        """
        try:
            df: pd.DataFrame = data
            df.fillna("", inplace=True)
            return df.to_dict(orient="records")
        except StopIteration:
            return []


    def normalize(self, data: Any)-> ...:
        print('Inside normalize')
        return data
        
    
    def execute(self, parameters) -> None:
        """
            This is the execution loop for the extraction
            :param data is a Dataframe that represents the data that is to be 
            ingested. It will go through several layers before persisting
            First we ingest the CSV and create a dataframe based on that
            We then do data cleaning within the transform layer and do checks along the way
            Finally, we can now persist to a POSTGRES DB in which everything is parsed out 
            dyamincally 

            Return None
        """
        print("inside execute")
        data = parameters[DATA]
        while True:
            try:
                # extract
                extraction_data = self.extract(data)
                if extraction_data is None:
                    print(f'Failed: Extraction Data-->{extraction_data}')
                    break
                
                print(f'Extraction Data-->{extraction_data}')
                # transform
                transfom_data, _ = self.transform(extraction_data)
                if transfom_data is None:
                    print(f'Failed: Extraction Data-->{transfom_data}')
                    break
                
                print(f'Transofrmed Data-->{transfom_data}')
                # load
                tx = self.load(transfom_data)

                if tx is None:
                    break
            except StopIteration:
                break
 

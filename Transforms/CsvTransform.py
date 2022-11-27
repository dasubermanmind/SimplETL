from typing import Any, List, Optional
import pandas as pd
from rich.console import Console

from Transforms.Optimus import Optimus

console = Console()

class CsvTransform(Optimus):
    
    def extract(self, csv: Optional[Any]) -> List[Any]:
        """
            The main entry point of the ETL. Within this phase we first setup
            all dependancies & any misc tasks we need to do before
            the pipeline begins

            Returns None
        """
        data = pd.read_csv(csv)
        return data


    def normalize(data: ...)-> ...:
        print('Inside normalize')
        
    
    def execute(self, data, data_source_name) -> None:
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
        while True:
            try:
                # extract
                extraction_data = self.extract(data)
                print(f'Extraction Data-->{extraction_data}')
                # transform
                transfom_data, _ = self.transform(extraction_data)
                print(f'Transofrmed Data-->{transfom_data}')
                # load
                tx = self.load(transfom_data, data_source_name)

                if tx is None:
                    break
            except StopIteration:
                break
 
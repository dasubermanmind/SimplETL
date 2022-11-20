
from typing import Any, List, Optional, Tuple
import logging
from rich.console import Console
import pandas as pd
from sqlalchemy import create_engine

from db.postgres import connect, create_table_on_headers, insert


console = Console()


class CsvTransform:
    def __init__(self, project_name, environment, endpoint) -> None:
        self.project_name = project_name
        self.environment = environment
        self.endpoint = endpoint
        self.logger = logging.basicConfig(level=logging.DEBUG)

    def extract(self, csv: Optional[Any]) -> List[Any]:
        """
            The main entry point of the ETL. Within this phase we first setup
            all dependancies & any misc tasks we need to do before
            the pipeline begins

            Returns None
        """
        data = pd.read_csv(csv)
        return data

    
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

    
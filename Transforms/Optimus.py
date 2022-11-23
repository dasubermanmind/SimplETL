from abc import abstractclassmethod
import logging
from typing import Any, Dict, Tuple

import pandas as pd
from sqlalchemy import create_engine
import uuid
from db.postgres import connect, create_table_on_headers, insert


"""
    This is the Base Class that all Transformers that will inherit from
    
    Extract functionality will be done done within the child class.
    Transform: Should be (will) be generic enough for all child classes but
    Each datum should be done within a normalize helper that each child class will implement themselves.
    Load: Should Also be generic 
"""


class Optimus:    
    def __init__(self, project_name, environment, endpoint):
        self.project_name = project_name
        self.environment = environment
        self.endpoint = endpoint
        self.logger = logging.basicConfigZ(level=logging.DEBUG)

    # These are part of the contract. These must be implemented for each transformer
    @abstractclassmethod
    def normalize(self , data: ...):
       raise NotImplementedError
    
    
    @abstractclassmethod
    def extract(self):
        raise NotImplementedError
    
    # TODO: Figure out what the generic fn signature should be
    def transform(self, data) -> ...:
        """
        This layer is responsible for extracting out column headers, data cleaning & any misc tasks 
        that are needed

        :param data Dataframe

        Returns Dataframe

        """
        if len(data) <= 0 or data is None:
            self.logger.info('Failed to extract properly')

        # Transform doc is the generic transform that will be used for all of the 
        # transformers, the impl for the other transformers should have a normalize fn 
        # that takes care of the batched datum.
        transform_doc: Dict[str, Any] = {}
        
        df = pd.DataFrame(data)
        column_headers = list(df.columns.values)

        df.fillna('No data provided at this time', inplace=True)
        df.columns = column_headers
        
        results = self.normalize_df(df) 
        
        transform_doc = {
            'main_data': results,
            '_id_': uuid.uuid4().hex 
        }
        
        print(transform_doc)

        # TODO: Investigate if I should cast transform_doc to a list or something (list[tuples])
        return transform_doc, column_headers

    
    
    def load(self, data, name) -> None:
        """
            :param data Dictionary

            Returns None
        """
        db = create_engine(connect())
        print(f'Sql Alchemy Engine up and running...{db}')
        create_table_on_headers(data, db, name)
        # insert next
        insert(db, data, name)
        # After inserted we can now "Finish" the ETL and give back statistics on the ingest
        return None



from abc import abstractclassmethod
from collections import defaultdict
import logging
from typing import Any, Dict, Tuple

import pandas as pd
from sqlalchemy import create_engine
import uuid
from db.postgres import connect, create_table_on_headers, insert
from settings.general import ID, MAIN_DATA


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
    
    
    """
        The return signature should be as follows
        
        JSON->
        {
            main_data: {
                pd.Dataframe data that is associated with the ingest
            }
            id: some generic id 
            ...
            potentially---meta_data:{
                another default dict that aggregates other pieces of data
                like user_name, where it was ran etc
            }
        }
    """
    def transform(self, data) -> ...:
        """
        This layer is responsible for extracting out column headers, data cleaning & any misc tasks 
        that are needed

        :param data Dataframe

        Returns Dataframe

        """
        if len(data) <= 0 or data is None:
            self.logger.info('Failed to extract properly')

        # # Transform doc is the generic transform that will be used for all of the 
        # # transformers, the impl for the other transformers should have a normalize fn 
        # # that takes care of the batched datum.
        transform_doc: Dict[str, Any] = defaultdict(str, Any)
        
        df = pd.DataFrame(data)
        column_headers = list(df.columns.values)

        df.fillna('No data provided at this time', inplace=True)
        df.columns = column_headers
        
        results = self.normalize_df(df) 
        # # TODO: ID should actually be a cryptographic key to avoid collisions
        transform_doc = {
            MAIN_DATA: results,
            ID: uuid.uuid4().hex 
        }
        
        # print(transform_doc)

        # # TODO: Investigate if I should cast transform_doc to a list or something (list[tuples])
        # return transform_doc, column_headers

    
    
    def load(self, data, name) -> None:
        """
            :param data Dictionary

            Returns None
        """
        print('start the loading phase')
        db = create_engine(connect())
        #print(f'Sql Alchemy Engine up and running...{db}')
        #create_table_on_headers(data, db, name)
        # insert next
        #insert(db, data, name)
        # After inserted we can now "Finish" the ETL and give back statistics on the ingest
        return None



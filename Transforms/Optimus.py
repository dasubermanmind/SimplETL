from abc import abstractclassmethod
from typing import Tuple

import pandas as pd
from sqlalchemy import create_engine

from db.postgres import connect, create_table_on_headers, insert


"""
    This is the Base Class that all Transformers that will inherit from
    
    Extract functionality will be done done within the child class.
    Transform: Should be (will) be generic enough for all child classes but
    Each datum should be done within a normalize helper that each child class will implement themselves.
    Load: Should Also be generic 
"""


class Optimus:    
    def __init__(self):
        #TODO: Migrate all instance variables from csv
        pass

    # These are part of the contract. These must be implemented for each transformer
    @abstractclassmethod
    def normalize(self , data):
       raise NotImplementedError
    
    
    @abstractclassmethod
    def extract(self):
        raise NotImplementedError
    
    def transform(self, data) -> Tuple[pd.DataFrame, list]:
        """
        This layer is responsible for extracting out column headers, data cleaning & any misc tasks 
        that are needed

        :param data Dataframe

        Returns Dataframe

        """

        if len(data) <= 0:
            self.logger.info('Failed to extract properly')

        df = pd.DataFrame(data)
        column_headers = list(df.columns.values)

        df.fillna('No data provided at this time', inplace=True)
        df.columns = column_headers

        print(df)
        # TODO: Update normalize fn
        # df = normalize_df(df)

        return df, column_headers

    
    
    def load(self, data, name) -> None:
        """
            :param data Dataframe

            Returns None
        """
        db = create_engine(connect())
        print(f'Sql Alchemy Engine up and running...{db}')
        create_table_on_headers(data, db, name)
        # insert next
        insert(db, data, name)
        # After inserted we can now "Finish" the ETL and give back statistics on the ingest
        return None



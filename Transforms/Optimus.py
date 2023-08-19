import abc
from abc import abstractclassmethod
from collections import defaultdict
import logging
from typing import Any, Dict, Tuple, Iterable

import pandas as pd
from sqlalchemy import create_engine
import uuid
from settings.general import ID, MAIN_DATA
from utilities.Utilities import Utilities

"""
    This is the Base Class that all Transformers that will inherit from
    
    Extract functionality will be done done within the child class.
    Transform: Should be (will) be generic enough for all child classes but
    Each datum should be done within a normalize helper that each child class will implement themselves.
    Load: Should Also be generic 
"""


class Optimus:    
    def __init__(self, project_name, environment):
        self.project_name = project_name
        self.environment = environment

    # These are part of the contract. These must be implemented for each transformer
    @abc.abstractmethod
    def normalize(self , data: Any):
       raise NotImplementedError
    
    @abc.abstractmethod
    def execute(self, data: Any):
        raise NotImplementedError

    @abc.abstractmethod
    def extract(self):
        raise NotImplementedError

    def transform(self, data: Iterable[Any]) -> ...:
        """
        This layer is responsible for extracting out column headers, data cleaning & any misc tasks 
        that are needed

        :param data Dataframe

        Returns Dataframe

        """
        lake_data = []
        normalized_data = {}

        for datum in data:
            try:
                # TODO: Update and split apart base class from impl class
                normalized_data = Utilities.normalize(datum)
            except Exception as e:
                print(f'Error Normalizing the data->{e}')

            if not normalized_data or len(normalized_data) <=0:
                continue
            else:
                lake_data.append(normalized_data)
        return lake_data

    
    
    def load(self, data, name) -> None:
        """
            :param data Dictionary

            Returns None
        """
        pass



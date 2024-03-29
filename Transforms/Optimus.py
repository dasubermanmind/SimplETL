import abc
from abc import abstractclassmethod
from typing import Any, Iterable

from es.Elastic import ElasticSearchWrapper
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
    def __init__(self, project_name, environment, endpoint, index_name):
        self.project_name = project_name
        self.environment = environment
        self.es_client = ElasticSearchWrapper()
        self.endpoint = endpoint
        self.index_name = index_name

    @abc.abstractmethod
    def normalize(self , data: Any):
       raise NotImplementedError
    
    @abc.abstractmethod
    def execute(self, data: Any):
        raise NotImplementedError

    @abc.abstractmethod
    def extract(self, data: Any):
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
                
                normalized_data = Utilities.normalize(datum)
            except Exception as e:
                print(f'Error Normalizing the data->{e}')

            if not normalized_data or len(normalized_data) <=0:
                continue
            else:
                lake_data.append(normalized_data)
        return lake_data
    

    def check_index_name(self,index_name):
        index = self.es_client.es_client.indices.exists(index=index_name)
        if not index:
            self.es_client.es_client.indices.create(index=index_name) # optionally I can add a mapping 


    def load(self, data):
        """
            :param data Dictionary

            Returns None
        """
        data_to_index = {}
        valid_success: int = 0
        valid_failure: int = 0

        def indexer(endpoint: str, success: int, fail: int):
            endpoint_stats = data_to_index.get(endpoint, {
                'success': 0,
                'fail': 0
            })

            endpoint_stats['success'] = success
            endpoint_stats['fail'] = fail
            data_to_index[endpoint] = endpoint_stats

        try:
            # create the index
            self.check_index_name(self.index_name)
            valid_success, valid_failure = self.es_client.load(data, index_name=self.index_name)

        except Exception as e:
            print(f'Failed to load: {e}')

        finally:
            indexer(self.endpoint,valid_success,valid_failure )
        return data_to_index




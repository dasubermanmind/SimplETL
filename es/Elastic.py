from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

from utilities.Utilities import Utilities



class ElasticSearchWrapper:
    def __init__(self, host='localhost', port=9200):
        self.host = host
        self.port = port
        self.es_client = Utilities.get_elastic_client()

    def load(self, data, index_name):
        valid_success: int = 0
        valid_fail: int = 0
        
        bulk_data = Utilities.set_data(data, index_name)


        for success, info in streaming_bulk(self.es_client, bulk_data,
                                            raise_on_error=False):
            if success:
                valid_success+=1
            else:
                valid_fail+=1
        
        return valid_success, valid_fail


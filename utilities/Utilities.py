
import logging
import requests
import pandas as pd

class Utilities:
    
    @staticmethod
    def _transform(data):
        pass
    
    @staticmethod 
    def getTransform(transform_type):
        pass

    @staticmethod
    def create_custom_logger(logger, filepath)-> logging.Logger:
        logger = logging.getLogger(__name__)
        
        stream_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(filepath)
        stream_handler.setLevel(logging.WARNING)
        file_handler.setLevel(logging.ERROR)
        
        # create custom formatters
        
        
        return logger
    
    @staticmethod
    def download_data_set(url: str)-> bool:
        r = requests.get(url)
        if r.status_code != 200:
            return {}
        
        return r.json()
    
    
    @staticmethod
    def write_to_csv(data, file_name: str)->None:
        df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in data.items() ]))
        return df.to_csv(file_name)
        

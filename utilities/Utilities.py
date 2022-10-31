
import logging
import requests

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
        

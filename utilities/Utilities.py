
import logging
import os
import zipfile
import requests
import pandas as pd
from zipfile import ZipFile

from settings.general import ZIP_EXTENSION


class Utilities:

    @staticmethod
    def _transform(data):
        pass

    @staticmethod
    def getTransform(transform_type):
        pass

    @staticmethod
    def create_custom_logger(logger, filepath) -> logging.Logger:
        logger = logging.getLogger(__name__)

        stream_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(filepath)
        stream_handler.setLevel(logging.WARNING)
        file_handler.setLevel(logging.ERROR)

        return logger

    @staticmethod
    def download_data_set(url: str) -> bool:
        r = requests.get(url)
        if r.status_code != 200:
            return {}

        return r.json()

    @staticmethod
    def write_to_csv(data, file_name: str) -> None:
        print('Write')
        df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in data.items()]))
        return df.to_csv(file_name)

    @staticmethod
    def extract_from_csv(file_name, logger) -> None:
        try:
            if not os.path.exists(file_name):
                os.makedirs(file_name)

            with zipfile.ZipFile('data/resultbook2.csv', 'r') as zipper:
                zipper.extractall('data/resultbook2.csv')
        except BaseException as e:
            logger(f'oops files had issues { e }')

        logger('Extraction done.')
        return None

    @staticmethod
    def normalize_df(data: pd.DataFrame):
        pass
    
    
    @staticmethod
    def remove_files(file_path: str)->None:
        try:
            if not os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            print('File not found')

    @staticmethod
    def preprocess():       
        path = 'data/'
        for filenames in os.listdir(path):
            if filenames.endswith(ZIP_EXTENSION):
                print("Zip file found")
                file_to_extract = path + filenames                
                with zipfile.ZipFile(file_to_extract, 'r') as z:
                    z.extractall('/data')
                    print('Extracted please check the data directory')
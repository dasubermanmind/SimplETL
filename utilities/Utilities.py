
import logging
import os
import uuid
import zipfile
from typing import Dict, Any, List, Optional
import elasticsearch

import requests
import pandas as pd
from zipfile import ZipFile

from settings.general import ZIP_EXTENSION, MAIN_DATA, NUMERICAL_DATA


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
    def numerical_data(data: Dict[str, Any], numeria_keys):
        numeria = {}

        for key in data.keys():
            try:
                if numeria_keys and key in numeria_keys.keys():
                    mp = numeria_keys[key]
                    numeria[mp] = float(data[key])
            except Exception as e:
                print(f'Error Parsing Numerical Data{e}')

        return numeria


    @staticmethod
    def main_data(data, skip_list: Optional[List[str]]= None):
        raw = data.copy()

        if skip_list:
            for key in skip_list:
                if key in raw:
                    del raw[key]

        return raw


    @staticmethod
    def normalize(data, parameters=None):
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
        # TODO: Prrune None vals
        # parse out different fields to normalize
        # Parse out main data
        # parse out numerical data
        numerical = Utilities.numerical_data(data, parameters)
        # parse out time data etc...
        main = Utilities.main_data(data)

        json_document = {
            MAIN_DATA: main,
            NUMERICAL_DATA: numerical
        }

        return json_document
    
    
    @staticmethod
    def remove_files(file_path: str)-> None:
        try:
            if not os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            print('File not found')

    @staticmethod
    def preprocess(data_file_path) -> ...:
        for filenames in os.listdir(data_file_path):
            if filenames.endswith(ZIP_EXTENSION):
                print("Zip file found")
                file_to_extract = data_file_path + filenames
                with zipfile.ZipFile(file_to_extract, 'r') as z:
                    z.extractall('/data')
                    print('Extracted please check the data directory')


    @staticmethod
    def get_elastic_client():
        env_hosts = os.getenv(ES_HOST_KEY)
        host: List[str] = [env_hosts]
        ports = os.getenv(ES_PORT)
        client = elasticsearch.Elasticsearch(host, port=ports)

        return client
    

    @staticmethod
    def set_datum(datum, index_name):
        try:
            record = {
                '_id': uuid.uuid4(),
                '_index': index_name,
                '_source': datum,
                '_type': 'doc'
            }
            return record
        except Exception as e:
            print(f'Error in setting the data: {datum}')


    
    @staticmethod
    def set_data(data, index_name):
        for data_source in data:
            datum = Utilities.set_datum(data_source, index_name)

            if datum:
                yield datum

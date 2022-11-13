
import pandas as pd

class JsonTransformer:
    # TODO: Need to create a base class and then have all my other transformers inherit.
    def __init__(self):
        pass
    
    def _start(self, json_file_path: str):
        df = pd.read_json(json_file_path)
        print(df)
    
    def execute(self):
        pass
    
    def extract(self):
        pass

    def transform(self):
        pass
    
    def load(self):
        pass

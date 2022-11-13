
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
        """
            Json Tranform will have two parts 1. Normalize 2. Apply
            Normalize will convert to a flat DS (a record) to be persisted to postgres
            The big question is whether or not I want to extract the subsets of each
            json structure 
            
            {
                'main':{
                    title: foo,
                    bar: {
                        nestedTitle: baz,
                        data: {
                            [...] <---This would be omitted unless if we performa deep traversal
                        }
                    }
                }
            } 
        
        """
        pass
    
    def load(self):
        pass

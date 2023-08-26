from typing import Any, List, Dict, Tuple
import pandas as pd

from Transforms.Optimus import Optimus
from settings.general import DATA


class CsvTransform(Optimus):

    def __init__(self, project_name, env, endpoint, index_name):        
        super().__init__(project_name, env, endpoint, index_name )
        self.project_name = project_name
        self.environment = env
        self.endpoint = endpoint 
        self.index_name = index_name


    def extract(self, data: Any) -> Tuple[int,List[Any]]:
        """
            The main entry point of the ETL. Within this phase we first setup
            all dependancies & any misc tasks we need to do before
            the pipeline begins

            Returns dataframe
        """
        try:
            reader = pd.read_csv(data, chunksize=1000)
            df = pd.concat(reader)
            results = df.to_dict(orient="records")
            output_len = len(results)
            return output_len, results
        except StopIteration:
            return 0, []
        
    
    def execute(self, parameters: Dict[str, Any]) -> None:
        """
            This is the execution loop for the extraction
            :param parameters

            Return None
        """
        data = parameters[DATA]
        while True:
            try:
                # extract
                ll, extraction_data = self.extract(data)
                if extraction_data is None:
                    print(f'Failed: Extraction Data-->{extraction_data}')
                    break
                
                # transform
                transfom_data = self.transform(extraction_data)
                if transfom_data is None:
                    print(f'Failed: Extraction Data-->{transfom_data}')
                    break
                
                # load
                stats = self.load(transfom_data)
                success = stats[self.endpoint]['success']
                fail = stats[self.endpoint]['fail']


                if success + fail == ll or success == ll or fail == ll:
                    break
            except StopIteration:
                break
 

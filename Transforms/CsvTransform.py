from typing import Any, List, Dict, Tuple
import pandas as pd

from Transforms.Optimus import Optimus
from settings.general import DATA


class CsvTransform(Optimus):

    def __init__(self, project_name, env):        
        super().__init__(project_name, env)
        self.project_name = project_name
        self.environment = env


    def extract(self, data: Any) -> Tuple[int,List[Any]]:
        """
            The main entry point of the ETL. Within this phase we first setup
            all dependancies & any misc tasks we need to do before
            the pipeline begins

            Returns dataframe
        """
        try:
            df: pd.DataFrame = data
            df.fillna("", inplace=True)
            df = df.to_dict(orient="records")
            output_len = len(df)
            return output_len, df
        except StopIteration:
            return []
        
    
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
                length, extraction_data = self.extract(data)
                if extraction_data is None:
                    print(f'Failed: Extraction Data-->{extraction_data}')
                    break
                
                print(f'Extraction Data-->{extraction_data}')
                # transform
                transfom_data = self.transform(extraction_data)
                if transfom_data is None:
                    print(f'Failed: Extraction Data-->{transfom_data}')
                    break
                
                print(f'Transofrmed Data-->{transfom_data}')
                # load
                stats = self.load(transfom_data)
                success = stats['success']
                fail = stats['fail']

                if success + fail == length or success == length or fail == length:
                    break
            except StopIteration:
                break
 

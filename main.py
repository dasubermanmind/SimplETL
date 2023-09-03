import glob
import os
from typing import Optional
import typer
from rich import print
from rich.console import Console
from rich.table import Table
from Transforms import CsvTransform
from es.Elastic import ElasticSearchWrapper

from settings.general import COVID_DIR, LOCAL_ENV, PROJECT_NAME, COVID_NAME, maryland, DATA, FILE_NAME
from utilities.Utilities import Utilities
import spacey
import en_core_web_sm



# Consts and appwide declarations
app = typer.Typer()
console = Console()

@app.command()
def intro():
    table = Table(
        'SimplETL a quick and dirty way to get your data transform and loaded to your database of choice')
    table.add_row("""
              .__                  .__   ______________________.____     
              ______|__|  _____  ______  |  |  \_   _____/\__    ___/|    |    
             /  ___/|  | /     \ \____ \ |  |   |    __)_   |    |   |    |    
             \___ \ |  ||  Y Y  \|  |_> >|  |__ |        \  |    |   |    |___ 
            /____  >|__||__|_|  /|   __/ |____//_______  /  |____|   |_______ \
                 \/           \/ |__|                  \/                    \/
                    """)
    table.add_row(
        'In order to start the ETL process first identify the data source. Please visit the readme for kaggle approved datasets')
    console.print(table)


# TODO Implement this next. 
@app.command()
def scraper():
    pass

@app.command()
def example(endpoint: str, index_name: str) -> None:
    """
        The main execution for handling all the different types of file formats.

        :params dataset, The optional CLI argument. This will be used when the user selects any of the
        options 1-4 and has a local copy of what they are wanting to ingest

        returns None

    """
    table = Table('What type of data set are you wanting to ingest?')

    typer.echo('Please make sure your zip file is in the data dir')
    # Utilities.preprocess(data_file_path)
    # Data has been pre-processed
    # Create the ETL Object
    es = ElasticSearchWrapper(index=index_name)
    ingest = CsvTransform.CsvTransform(maryland, 'dev', endpoint, index_name)
    # Execute
    parameters = {
        DATA: 'data/maryland.csv',
        FILE_NAME : maryland,
    }
    
    ingest.execute(parameters)

    results = es.get_all()

    nlp = spacey.load("en_core_web_sm")
    
    for doc in results['hits']['hits']:
        print(doc)
        d = nlp.load(doc)
        print([(w.text, w.pos_) for w in d])

    




if __name__ == '__main__':
    app()

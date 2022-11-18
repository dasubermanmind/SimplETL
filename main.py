import glob
import os
from typing import Optional
import typer
from rich import print
from rich.console import Console
from rich.table import Table
from Transforms.CsvTransform import CsvTransform
from Transforms.JsonTransformer import JsonTransformer

from settings.general import COVID_DIR, LOCAL_ENV, PROJECT_NAME,COVID_NAME
from utilities.Utilities import Utilities


# Consts and appwide declarations
app = typer.Typer()
console = Console()
zip = 'owid-covid-data.csv.zip'
maryland = 'maryland'

ZIP_EXTENSION = '*.csv.zip'
EXTENSION = ".csv"

@app.command()
def intro():
    table = Table('SimplETL a quick and dirty way to get your data transform and loaded to your database of choice')
    table.add_row("""
                   ____  _  _      ____  _     _____ _____  _    
                  / ___\/ \/ \__/|/  __\/ \   /  __//__ __\/ \   
                  |    \| || |\/|||  \/|| |   |  \    / \  | |   
                  \___ || || |  |||  __/| |_/\|  /_   | |  | |_/\
                   \____/\_/\_/  \|\_/   \____/\____\  \_/  \____/
                    """)
    table.add_row('In order to start the ETL process first identify the data source. Please visit the readme for kaggle approved datasets')    
    console.print(table)

@app.command()
def etl_example():
    maryland_data = 'data\covid\maryland-history.csv'
    
    name = maryland_data[-6:]
    
    etl = CsvTransform(PROJECT_NAME, LOCAL_ENV, None)
        
    etl.execute(maryland_data, name)
    console.print('All finished. Please check your local directory for results or the database that you stood up')
    
@app.command()
def start(dataset: Optional[str] =None):
    table = Table('What type of data set are you wanting to ingest?')
    table.add_row('1. CSV')
    table.add_row('2. JSON File')
    table.add_row('3. XML')
    table.add_row('4. Excel')
    
    console.print(table)
    selection: str = typer.prompt('Select 1-4')
    console.print(f'You selected { selection }')
    
    if selection.lower() == '1':
        csv = typer.prompt('Please place your csv within the data directory. What did you call it?')
        print(f'Input--->{csv}')
        # What we need to do here is basically parse out the dat dir and find the prefix of the file 
        # iterate through the data dir
        all_files_found = [file
                 for path, subdir, files in os.walk(csv)
                 for file in glob(os.path.join(path, EXTENSION))]
        
        print(f'Files Found--->{all_files_found}')
        
        # Extract the zip
        # Utilities.extract_from_csv(csv)
        # that the user entered and then parse it
        
        #etl = CsvTransform(PROJECT_NAME, LOCAL_ENV, None)
        
        
    # if selection.lower() == '2':
    #     etl = JsonTransformer()
    #     etl._start('test.json')
        
    # if selection.lower() == '3':
    #     print('This hasnt been implemented yet')
        
    # if selection.lower() == '4':
    #     print('This hasnt been implemented yet')
    
        
if __name__ == '__main__':
    app()
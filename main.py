from typing import Optional
import typer
from rich import print
from rich.console import Console
from rich.table import Table
from Transforms.CsvTransform import CsvTransform
from Transforms.JsonTransformer import JsonTransformer

from settings.general import COVID_DIR, LOCAL_ENV, PROJECT_NAME,COVID_NAME
from covid19dh import covid19

from utilities.Utilities import Utilities

# Consts and appwide declarations
app = typer.Typer()
console = Console()
zip = 'owid-covid-data.csv.zip'
maryland = 'maryland'

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
def etl_example(dataset: str):
    if dataset.lower() == maryland:
        maryland_data = 'data\covid\maryland-history.csv'
        console.print(f'Starting an ingest cyle on the { dataset } data set....please stand by')
        etl = CsvTransform(PROJECT_NAME, LOCAL_ENV, None)
        console.print('Retreiving covid data sets')
        
        etl._start(maryland_data)
        # If there were no errors then we display the goodbye message
        console.print('All finished. Please check your local directory for results or the database that you stood up')
    else:
        console.print(f'No Dataset provided. Did you want to run the maryland covid dataset?')
        
    
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
        csv = typer.prompt('')
        # TODO: Setup csv extraction...maybe put it into the transform class?
        etl = CsvTransform(PROJECT_NAME, LOCAL_ENV, None)
        
        
    if selection.lower() == '2':
        etl = JsonTransformer()
        etl._start('test.json')
        
    if selection.lower() == '3':
        print('This hasnt been implemented yet')
        
    if selection.lower() == '4':
        print('This hasnt been implemented yet')
    
        
if __name__ == '__main__':
    app()
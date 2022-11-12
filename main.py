import typer
from rich import print
from rich.console import Console
from rich.table import Table

from interface.SimplETL import SimplETL
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
        etl = SimplETL(PROJECT_NAME, LOCAL_ENV, None)
        console.print('Retreiving covid data sets')
        
        etl._start(maryland_data)
        # finished
        console.print('All finished')
    # else:
    #     console.print(f'Starting an ingest cyle on the { covid } data set....please stand by')
    #     Utilities.extract_from_csv(COVID_DIR + zip)
    #     etl = SimplETL(PROJECT_NAME, LOCAL_ENV, None)
    
@app.command()
def start(dataset: str):
    table = Table('What type of data set are you wanting to ingest?')
    table.add_row('CSV')
    table.add_row('JSON File')
    
        
if __name__ == '__main__':
    app()
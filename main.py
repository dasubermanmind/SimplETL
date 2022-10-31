import typer
from rich import print
from rich.console import Console
from rich.table import Table

from interface.SimplETL import SimplETL
from settings.general import LOCAL_ENV, PROJECT_NAME
from covid19dh import covid19

from utilities.Utilities import Utilities

app = typer.Typer()
console = Console()

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
    console.print('Starting an ingest cyle....please stand by')
    etl = SimplETL(PROJECT_NAME, LOCAL_ENV, None)
    console.print('Retreiving covid data sets')
    covid_url = 'https://coronavirus.data.gov.uk/api/v2/openapi.json'
    data = Utilities.download_data_set(covid_url)
    
    if not data:
        console.print('Failed to download Exiting...')
        raise typer.Exit()
    
    etl._start(data)
    # finished
    console.print('All finished')
    
if __name__ == '__main__':
    app()
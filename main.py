import typer
from rich import print
from rich.console import Console
from rich.table import Table

from interface.SimplETL import SimplETL
from settings.general import LOCAL_ENV, PROJECT_NAME
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
    covid_url = ''
    console.print('Starting an ingest cyle....please stand by')
    etl = SimplETL(PROJECT_NAME, LOCAL_ENV, None)
    # download the data into a csv
    console.print('Retreiving covid data sets')
    done = Utilities.download_data_set(covid_url)
    
    if not done:
        console.print('Failed to download Exiting...')
        raise typer.Exit()
    
    while True:
        ingest = etl._start()
        if not ingest:
            break
        
        
    # finished
    console.print('All finished')
    
    

if __name__ == '__main__':
    app()
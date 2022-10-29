import typer
from rich import print
from rich.console import Console
from rich.table import Table

from interface.AbstractSimplETL import AbstractSimplETL
from settings.general import LOCAL_ENV, PROJECT_NAME
from utilities.Utilities import Utilities

app = typer.Typer()
console = Console()

@app.command()
def intro():
    table = Table('SimplETL a quick and dirty way to get your data transform and loaded to your database of choice')
    table.add_row("""\ _________.__               .__  ______________________.____     
 /   _____/|__| _____ ______ |  | \_   _____/\__    ___/|    |    
 \_____  \ |  |/     \\____ \|  |  |    __)_   |    |   |    |    
 /        \|  |  Y Y  \  |_> >  |__|        \  |    |   |    |___ 
/_______  /|__|__|_|  /   __/|____/_______  /  |____|   |_______ \
        \/          \/|__|                \/                    \/
                    """)
    table.add_row('In order to start the ETL process first identify the data source. Please visit the readme for kaggle approved datasets')    
    console.print(table)

@app.command()
def etl_example():
    print('Starting an ingest cyle....please stand by')
    etl = AbstractSimplETL(PROJECT_NAME, LOCAL_ENV, None)
    # download the data into a csv
    done = Utilities.download_data_set()
    
    if not done:
        console.print('Failed to download Exiting...')
        raise typer.Exit()
    etl._start()
    
    

if __name__ == '__main__':
    app()
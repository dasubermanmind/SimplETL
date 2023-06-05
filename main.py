import glob
import os
from typing import Optional
import typer
from rich import print
from rich.console import Console
from rich.table import Table
from Transforms import CsvTransform

from settings.general import COVID_DIR, LOCAL_ENV, PROJECT_NAME, COVID_NAME
from utilities.Utilities import Utilities


# Consts and appwide declarations
app = typer.Typer()
console = Console()
covid_zip = 'owid-covid-data.csv.zip'
maryland = 'maryland'

ZIP_EXTENSION = '*.csv.zip'
EXTENSION = ".csv"


@app.command()
def intro():
    table = Table(
        'SimplETL a quick and dirty way to get your data transform and loaded to your database of choice')
    table.add_row("""
                   ____  _  _      ____  _     _____ _____  _
                  / ___\\/ \\/ \\__/|/  __\\/ \\   /  __//__ __\\/ \
                  |    \\| || |\\/|||  \\/|| |   |  \\    / \\  | |
                  \\___ || || |  |||  __/| |_/\\|  /_   | |  | |_/\
                   \\____/\\_/\\_/  \\|\\_/   \\____/\\____\\  \\_/  \\____/
                    """)
    table.add_row(
        'In order to start the ETL process first identify the data source. Please visit the readme for kaggle approved datasets')
    console.print(table)

@app.command()
def etl() -> None:
    """
        The main execution for handling all the different types of file formats.

        :params dataset, The optional CLI argument. This will be used when the user selects any of the
        options 1-4 and has a local copy of what they are wanting to ingest

        returns None

    """
    table = Table('What type of data set are you wanting to ingest?')
    table.add_row('1. CSV')

    console.print(table)
    selection: str = typer.prompt('Select 1-4')
    console.print(f'You selected { selection }')

    if selection.lower() == '1':
        # Pre-process Stage
        typer.echo('Please make sure your zip file is in the data dir')
        Utilities.preprocess()
        # Data has been pre-processed
        # Create the ETL Object
        etl = CsvTransform.CsvTransform(maryland, 'dev')
        # Execute
        etl.execute('data/m.csv', maryland)
    else:
        print('We only support CSV right now sorry')


if __name__ == '__main__':
    app()

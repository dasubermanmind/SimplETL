import os
import psycopg2
from dotenv import load_dotenv

database = os.getenv('POSTGRES_DB')
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
host = 'localhost'

load_dotenv()

class LoadPostgres:
    def __init__(self) -> None:
        connection = psycopg2.connect(f'host={host}, dbname={database}, user={user}, password={password}')
        
        if connection:
            print('Connected to the Postgres DB!')
            # create a cursor
            # cursor = connection.cursor()
            # cursor.execute('SELECT version()')
        
            # cursor.close()
        
    def insert(self):
        pass
        
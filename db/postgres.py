import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from db.general import POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER

load_dotenv()

def _connection()->str:
        user_spec = f'{os.getenv(POSTGRES_USER)}:{os.getenv(POSTGRES_PASSWORD)}'
        host_spec = os.getenv(POSTGRES_HOST)
        port = os.getenv(POSTGRES_PORT)
        if port:
            host_spec += f':{port}'
        schema = f'/{os.getenv(POSTGRES_DB)}' if os.getenv(POSTGRES_DB) else ''
        return f'postgres://{user_spec}@{host_spec}{schema}'


def create_connection():
    connection = psycopg2.connect(_connection())
    cursor = connection.cursor()
    print(connection.get_dsn_parameters())   
    cursor.execute('SELECT version();')
    record = cursor.fetchone()
    print(f'You connected to --> {record}')

    
def insert(connection, data: pd.DataFrame)->None:
    # gather keys
    dKeys = ', '.join(data.keys())
    # gather vals
    dVals = ', '.join(['%%(%s)s' % x for x in data])
    sql = 'INSERT INTO {0}({1}) VALUES({2}) RETURNING id'.format(POSTGRES_DB,dKeys,dVals)
    with connection.cursor() as cursor:
        cursor.execute(sql, data)
        response = cursor.fetchone()
        cursor.commit()
        return response['id']
    
            
            
        
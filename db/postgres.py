import os
import psycopg2
from dotenv import load_dotenv
from db.general import POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER

load_dotenv()

"""
The connection string can be represented as dictionary as this
 param_dic = {
    "host"      : str(host),
    "database"  : str(database),
    "user"      : str(user),
    "password"  : str(password)
    }
"""

def _connection()->str:
        user_spec = f'{os.getenv(POSTGRES_USER)}:{os.getenv(POSTGRES_PASSWORD)}'
        host_spec = os.getenv(POSTGRES_HOST)
        port = os.getenv(POSTGRES_PORT)
        if port:
            host_spec += f':{port}'
        schema = f'/{os.getenv(POSTGRES_DB)}' if os.getenv(POSTGRES_DB) else ''
        return f'postgres://{user_spec}@{host_spec}{schema}'


def connect()->...:
    try:
        # Connect to the db
        connection = psycopg2.connect(_connection())
        cursor = connection.cursor()
        print(connection.get_dsn_parameters())   
        cursor.execute('SELECT version();')
        record = cursor.fetchone()
        print(f'You connected to --> {record}')
        
    except (Exception, psycopg2.Error) as error:
        print(error)
    return connection
            
def create_table_on_headers(data, engine, tablename):
    if not engine.dialect.has_table(engine, tablename):
        data.to_sql(tablename, engine, index=False)
        
def insert(engine, data, tablename):
    with engine.connect().execution_options(autocommit=True) as conn:
        data.to_sql(tablename, con=conn, if_exists='append', index= False)
        
            
        
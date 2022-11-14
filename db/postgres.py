import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import psycopg2.extras as psql_extras
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


def create_and_insert(data,headers)-> None:
    try:
        # Connect to the db
        connection = psycopg2.connect(_connection())
        cursor = connection.cursor()
        print(connection.get_dsn_parameters())   
        cursor.execute('SELECT version();')
        record = cursor.fetchone()
        print(f'You connected to --> {record}')

        # TODO: Refactor this Create/Insert
        # Create the table
        def create_table(cursor, create_query):
            try:
                cursor.execute(create_query)
            except Exception as error:
                print(error)
        
        table_create_sql = """
                CREATE TABLE maryland (
                id SERIAL PRIMARY KEY
            )
         """
          
        create_table(cursor, table_create_sql)
        
        cursor.execute('ALTER TABLE maryland ADD COLUMN newcol text;')
        connection.commit()
        for k,v in data.items():
            cursor.execute('''UPDATE maryland 
                                SET newcol = (%s) 
                            WHERE col1 = (%s);''',(v,k))
            connection.commit()
        
                
        # Insert
        dcols = data.keys()
        # gather vals
        dVals = [data[col] for col in dcols]
        vals_str_list = ["%s"] * len(dVals)
        vals_str = ", ".join(vals_str_list)
        sql = 'INSERT INTO {0}({1}) VALUES({2})'.format('maryland', dcols, vals_str)
        try:
            cursor.execute(sql, data)    
        except Exception as e:
            print(e)
        
        
    except (Exception, psycopg2.Error) as error:
        print(error)
    finally:
        connection.commit()
        cursor.close()

            
            
        
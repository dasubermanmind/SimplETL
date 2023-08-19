import sqlite3

from sqlalchemy import create_engine

class Migration:
    def __init__(self, db_path, query):
        self.db_path = db_path
        self.query = query
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def run_query(self):
        self.cursor.execute(self.query)
        self.conn.commit()

    def close(self):
        self.conn.close()

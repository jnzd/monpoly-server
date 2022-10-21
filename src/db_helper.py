import psycopg2 as pg
import run_command
from pathlib import Path
import os

class DbHelper:
    def __init__(self,
                 user='admin',
                 password='quest',
                 host='questdb',
                 port=8812,
                 database='qdb'):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.sql_drop_tables = ''
        self.signature_file = ''
        # self.empty = True


    def make_connection(self):
        connection = pg.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            # The following two options are needed when connecting from windows
            gssencmode='disable',
            sslmode='disable')
        return connection

    def run_query(self, query: str, select: bool = False, verbose: bool = True):
        '''
        Runs the given SQL query on the database
        '''
        if verbose:
            print(f'Running query: {query}')

        connection = None
        cursor = None
        try:
            connection = self.make_connection()
            cursor = connection.cursor()
            cursor.execute(query)
            if select:
                return cursor.fetchall()

        finally:
            if cursor: cursor.close()
            if connection: connection.close()
            if verbose: print('Postgres connection is closed.')


        
        
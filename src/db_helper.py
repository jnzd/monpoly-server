import psycopg2 as pg
import run_command
from pathlib import Path
import os

class db_helper:
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
        self.empty = True


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


    def is_empty(self):
        return self.empty
        # tables = self.run_query("SHOW TABLES;", select=True)
        # hacky solution as it relies on there always being 3 hidden tables created by QuestDB
        # return tables is not None and len(list(tables)) > 3


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


    def create_database(self, sig_file: str, windows: bool = False):
        '''
        Creates a database from the given signature file
        '''
        if not self.is_empty():
            print("""ERROR: There are already tables in the database. Please delete them before creating a new database or use a different QuestDB instance \nNo tables were created""")
            return {'error': 'There are already tables in the database. Please delete them before creating a new database or use a different QuestDB instance, No tables were created'}
        elif sig_file == '':
            print("""ERROR: no signature file provided""")
            return {'error': 'no signature file provided'}

        print(f'Creating database from {sig_file}')
        # TODO: call monpoly with `monpoly -sql sig_file`
        path = Path(sig_file)
        self.signature_file = path
        parent = path.parent.absolute()
        filename = path.name
        cmd_create = ''
        cmd_drop = ''
        if windows:
            # TODO: make this less hacky; probably need to run monpoly on a server and communicate over a network protocol
            cmd_create = f'docker run -v {parent}:/sigs monpoly:sql-0.1 -sql /sigs/{filename}'
            cmd_drop = f'docker run -v {parent}:/sigs monpoly:sql-0.1 -sql_drop /sigs/{filename}'
        else:
            cmd_create = f'monpoly -sql {path}'
            cmd_drop = f'monpoly -sql_drop {path}'

        query_create = run_command.runcmd(cmd_create, verbose = False)
        query_drop = run_command.runcmd(cmd_drop, verbose = False)
        self.run_query(query_create)
        self.empty = False
        self.sql_drop_tables = query_drop
        drop_file = open('./sql/drop.sql', 'w') 
        drop_file.write(query_drop)
        drop_file.close()

        return query_create


    # def delete_database(self, sig_file: str, windows: bool = False):
    def delete_database(self):
        '''
        Deletes the database associated with the given signature file
        '''
        if self.is_empty():
            if self.sql_drop_tables == '' and os.path.exists('./sql/drop.sql'):
                drop_file = open('./sql/drop.sql', 'r')
                self.sql_drop_tables = drop_file.read()
                drop_file.close()
            else:
                print("""ERROR: There are no tables in the database.\nNothing to delete""")
                return {'error': 'There are no tables in the database. Nothing to delete',
                        'self.sql_drop_tables': self.sql_drop_tables,
                        'os.path.exists': os.path.exists('./sql/drop.sql'),
                        'ls': os.listdir('./sql')}

        
        print(f'Deleting tables associated with {self.signature_file}')

        # TODO prompt user before running this query and deleting all tables
        self.run_query(self.sql_drop_tables)
        os.remove('./sql/drop.sql')
        self.empty = True
        return{'query ran': self.sql_drop_tables}
        
        
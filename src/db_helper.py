import psycopg2 as pg
import cli
from pathlib import Path

class db_helper:
    def make_connection(self):
        connection = pg.connect(
            user='admin',
            password='quest',
            host='127.0.0.1',
            port='8812',
            database='qdb',
            gssencmode='disable',
            sslmode='disable')
        return connection


    def database_is_empty(self):
        tables = self.run_query("SHOW TABLES;", select=True)
        # hacky solution as it relies on there always being 3 hidden tables created by QuestDB
        return tables is not None and len(list(tables)) > 3


    def run_query(self, query: str, select: bool = False, verbose: bool = True):
        '''
        Runs the given query on the database
        '''
        if verbose:
            print(f'Running query: {query}')
        connection = None
        cursor = None
        try:
            # TODO: handle already existing tables ('CREATE TABLE IF NOT EXISTS') this would need to be done in log_parser.ml
            connection = self.make_connection()
            cursor = connection.cursor()
            # text-only query
            cursor.execute(query)
            if select:
                return cursor.fetchall()

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
            if verbose:
                print('Postgres connection is closed.')

    def create_database(self, sig_file: str):
        '''
        Creates a database from the given signature file
        '''
        if self.database_is_empty():
            print("""ERROR: There are already tables in the database. Please delete them before creating a new database or use a different QuestDB instance \nNo tables were created""")
            return

        print(f'Creating database from {sig_file}')
        # TODO: call monpoly with `monpoly -sql sig_file`
        path = Path(sig_file)
        parent = path.parent.absolute()
        filename = path.name
        # TODO: make this less hacky; probably need to run monpoly on a server and communicate over a network protocol
        query = cli.runcmd(f'docker run -v {parent}:/sigs monpoly:sql-0.1 -sql /sigs/{filename}', verbose = False)
        self.run_query(query)


    def delete_database(self, sig_file: str):
        '''
        Deletes the database associated with the given signature file
        '''
        if not self.database_is_empty():
            print("""ERROR: There are not tables in the database.\nNothing to delete""")
            return
        
        print(f'Deleting database associated with {sig_file}')
        path = Path(sig_file)
        parent = path.parent.absolute()
        filename = path.name
        # TODO: make this less hacky; probably need to run monpoly on a server and communicate over a network protocol
        query = cli.runcmd(f'docker run -v {parent}:/sigs monpoly:sql-0.1 -sql_drop /sigs/{filename}', verbose = False)
        # TODO prompt user before running this query and deleting all tables
        self.run_query(query)
        
        
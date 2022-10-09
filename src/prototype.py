import argparse
import subprocess
from pathlib import Path
import psycopg2 as pg


def runcmd(cmd, verbose = False, *args, **kwargs):

    process = subprocess.Popen(
        cmd,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        text = True,
        shell = True
    )
    std_out, std_err = process.communicate()
    if verbose:
        print(std_out.strip(), std_err)
    return std_out.strip()


def create_database(sig_file: str):
    '''
    Creates a database from the given signature file
    '''
    print(f'Creating database from {sig_file}')
    # TODO: call monpoly with `monpoly -sql sig_file`
    path = Path(sig_file)
    parent = path.parent.absolute()
    filename = path.name
    # print(f'docker run -v {parent}:/sigs monpoly:sql-0.1 -sql /sigs/{filename}')
    # TODO: make this less hacky; probably need to run monpoly on a server and communicate over a network protocol
    query = runcmd(f'docker run -v {parent}:/sigs monpoly:sql-0.1 -sql /sigs/{filename}', verbose = True)
    connection = None
    cursor = None
    try:
        connection = pg.connect(
            user='admin',
            password='quest',
            host='127.0.0.1',
            port='8812',
            database='qdb',
            gssencmode='disable',
            sslmode='disable')
        # TODO: handle already existing tables ('CREATE TABLE IF NOT EXISTS') this would need to be done in log_parser.ml
        cursor = connection.cursor()
        # text-only query
        cursor.execute(query)

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print('Postgres connection is closed.')




def log_events(events: list):
    '''
    logs the given events in the database
    '''
    print(f'Logging events: {events}')

def main():
    parser = argparse.ArgumentParser()

    # --create-database <signature-file> --log-event events 
    parser.add_argument("-c",
                        "--create-database",
                        dest = "signature",
                        help="Takes the path to a signature file and creates a corresponding database"
                       )

    parser.add_argument("-l", 
                        "--log-event",
                        nargs='*',
                        dest = "events",
                        help="Takes a list of events and adds them to the database with the current time"
                       )

    args = parser.parse_args()

    if(args.signature):
        create_database(args.signature)

    if(args.events):
        log_events(args.events)


if __name__ == "__main__":
    main()


# parser = argparse.ArgumentParser()
# parser.parse_args()
# print(type(sys.argv))
# print('The command line arguments are:')
# for i in sys.argv:
#     print(i)

# import sys
# import getopt
# import datetime as dt

# connection = None
# cursor = None
# try:
#     connection = pg.connect(
#         user='admin',
#         password='quest',
#         host='127.0.0.1',
#         port='8812',
#         database='qdb',
#         gssencmode='disable',
#         sslmode='disable')
#     cursor = connection.cursor()

#     if()
#     # text-only query
#     cursor.execute('''CREATE TABLE IF NOT EXISTS trades (
#         ts TIMESTAMP, date DATE, name STRING, value INT)
#         timestamp(ts);''')

#     # insert 10 records
#     for x in range(10):
#         now = dt.datetime.utcnow()
#         date = dt.datetime.now().date()
#         cursor.execute('''
#             INSERT INTO trades
#             VALUES (%s, %s, %s, %s);
#             ''',
#             (now, date, 'python example', x))

#     # commit records
#     connection.commit()

#     cursor.execute('SELECT * FROM trades;')
#     records = cursor.fetchall()
#     for row in records:
#         print(row)

# finally:
#     if cursor:
#         cursor.close()
#     if connection:
#         connection.close()
#     print('Postgres connection is closed.')
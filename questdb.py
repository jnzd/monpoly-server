import psycopg2 as pg
import datetime as dt

connection = None
cursor = None
try:
    connection = pg.connect(
        user='admin',
        password='quest',
        host='127.0.0.1',
        port='8812',
        database='qdb')
    cursor = connection.cursor()

    # text-only query
    cursor.execute('''CREATE TABLE IF NOT EXISTS trades (
        ts TIMESTAMP, date DATE, name STRING, value INT)
        timestamp(ts);''')

    # insert 10 records
    for x in range(10):
        now = dt.datetime.utcnow()
        date = dt.datetime.now().date()
        cursor.execute('''
            INSERT INTO trades
            VALUES (%s, %s, %s, %s);
            ''',
            (now, date, 'python example', x))

    # commit records
    connection.commit()

    cursor.execute('SELECT * FROM trades;')
    records = cursor.fetchall()
    for row in records:
        print(row)

finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()
    print('Postgres connection is closed.')
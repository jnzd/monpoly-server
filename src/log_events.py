from db_helper import db_helper
from questdb.ingress import Sender
from datetime import datetime

db = db_helper()

def log_events(events: list, timestamp_str: str = ""):
    '''
    logs the given events in the database
    '''
    # TODO: 
    if timestamp_str == "":
        timestamp = datetime.now()
    else:
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')

    print(f'Logging events: {events} at {timestamp}')

    with Sender('localhost', 9009) as sender:
        sender.row(
            'sensors',
            symbols={'id': 'toronto1'},
            columns={'temperature': 20.0, 'humidity': 0.5})
        sender.flush()
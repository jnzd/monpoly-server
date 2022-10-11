from db_helper import db_helper
import questdb

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
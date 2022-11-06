from datetime import datetime

timestamp_fmt: str = '%Y-%m-%d %H:%M:%S.%f'

def get_timestamp(event: dict, timestamp_now: datetime) -> int:
    '''
    This method checks if the event has a timestamp
    it sets it to the current time if it doesn't
    It returns a timestamp in seconds since 1970-01-01 00:00:00 UTC
    (in monpoly/scr/formula_parser.mly:timeunits it can be seen that 
    seconds are the smallest and default time unit in monpoly)
    '''
    if 'timestamp' in event.keys():
        ts = datetime.strptime(event['timestamp'], timestamp_fmt)
    else:
        ts = timestamp_now
    return int(ts.timestamp())
    
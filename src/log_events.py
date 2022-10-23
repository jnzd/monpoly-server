from db_helper import DbHelper
from questdb.ingress import Sender
from questdb.ingress import Buffer
from datetime import datetime
from monitor import Monitor
from pathlib import Path
from run_command import runcmd
import json

def json_to_log(events) -> str:
    return ''

def store_events_in_db(mon: Monitor, events_json: str):
    '''
    stores the given events in the database
    '''
    return ''

def send_events_to_monpoly(mon, events_json):
    with open(events_json) as f:
        events = json.load(f)
        log_for_monpoly = json_to_log(events)
        return dict()

def log_events(mon: Monitor, events_json: str):
    '''
    logs the given events in the database
    '''
    buf = Buffer()
    with open(events_json) as f:
        events = json.load(f)
        for e in events:
            print('loop level 1')
            if 'timestamp' in e.keys():
                print('reading timestamp')
                ts = datetime.strptime(e['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
            else:
                print('getting current time')
                ts = datetime.now()
            print(f'ts: {ts}')
            if 'predicates' not in e.keys():
                return {'message': 'no predicates in event'}
            for p in e['predicates']:
                print('loop level 2')
                if 'name' not in p.keys():
                    return {'log_events error': 'predicate must have a "name"'}
                elif 'occurrences' not in p.keys():
                    print(f'no occurrences, p.keys(): {p.keys()}')
                    # predicate can be named without an occurrence
                    break
                name = p['name']
                print(f'name = {name}')
                for occ in p['occurrences']:
                    print('loop level 3')
                    columns = dict()
                    for i, o in enumerate(occ):
                        print('loop level 4')
                        # column names in questdb go from x1 to xn
                        columns |= {f'x{i+1}': o}
                    buf.row(
                        name,
                        symbols = None,
                        columns = columns,
                        at = ts
                    )
                    print('added row to buffer')
        with Sender(mon.db.host, 9009) as sender:
            print(f'< flushing buffer: {buf} >')
            # TODO DON'T CREATE COLUMN IF IT DOESN'T EXIST
            sender.flush(buf)
        log_for_monpoly = json_to_log(events)

        mon.clear_directory(mon.events_dir)
        return {'events': events}
        # db_log = store_events_in_db(mon, events, ts)
        # monpoly_log = send_events_to_monpoly(mon, events_json)
    
    return db_log | monpoly_log

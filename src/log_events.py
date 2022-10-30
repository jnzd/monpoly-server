from db_helper import DbHelper
from questdb.ingress import Sender
from questdb.ingress import Buffer
from datetime import datetime
from monitor import Monitor
from pathlib import Path
from run_command import runcmd
import json

timestamp_fmt: str = '%Y-%m-%d %H:%M:%S.%f'

def check_predicate(pred):
    '''
    This method checks if the given predicate is valid
    '''
    # TODO implement this
    return True

def make_string_from_occurrence(occ):
    attributes = ','.join(str(x) for x in occ)
    print(f'attributes: {attributes}')
    attributes = f'({attributes})'
    return attributes

def make_string_from_pred(predicate):
    name = predicate['name']
    occurrences = [make_string_from_occurrence(o) for o in predicate['occurrences']]
    # check if the predicates are valid
    for o in occurrences:
        if not check_predicate(o):
            return {'error': f'{name} {o}'}
    occ_string = ' '.join(occurrences)
    return {'success': f'{name} {occ_string}'}

def make_string_from_event(event):
    ts = event['timestamp-int']
    predicates = event['predicates']
    pred_str_list = []
    for p in predicates:
        p_str = make_string_from_pred(p)
        if 'error' in p_str.keys():
            return {'error': f"The predicate: {p_str['error']} at timestamp {datetime.fromtimestamp(ts)} is invalid."}
        pred_str_list.append(p_str['success'])
    pred_str = ' '.join(pred_str_list)
    return {'success': f'@{int(ts)} {pred_str};'}

def create_log_string(events):
    '''
    takes an ordered list of events and returns a string that can be used as input for monpoly
    '''
    event_strings_list = []
    for e in events:
        e_str = make_string_from_event(e)
        if 'error' in e_str.keys():
            return {'error': e_str['error']}
        else:
            event_strings_list.append(e_str['success'])
    event_string = ' '.join(event_strings_list)
    return {'success': event_string}

    return '@10 P (1); @20 P (1);  @30 P (2) Q (1,a); @40 Q (2,b); @100'

def get_timestamp(event, timestamp_now):
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
    
def store_events_in_db(mon: Monitor, events):
    '''
    logs the given events in the database
    '''
    buf = Buffer()
    for e in events:
        ts = datetime.fromtimestamp(e['timestamp-int'])
        for p in e['predicates']:
            if 'name' not in p.keys():
                return {'log_events error': 'predicate must have a "name"'}
            elif 'occurrences' not in p.keys():
                # predicate can be named without an occurrence
                break
            name = p['name']
            for occ in p['occurrences']:
                columns = dict()
                for i, o in enumerate(occ):
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
        sender.flush(buf)

    return {'events': events}

def send_events_to_monpoly(mon: Monitor, events):
        monpoly_log_string = create_log_string(events)
        if 'error' in monpoly_log_string.keys():
            return {'error': f'Error while creating monpoly log string {monpoly_log_string["error"]}'}
        else:
            monpoly_log_string = monpoly_log_string['success']
        print(monpoly_log_string)
        if mon.monpoly:
            if mon.monpoly.stdin:
                mon.monpoly.stdin.write(monpoly_log_string)
                mon.monpoly.stdin.flush()
                return{'success': f'sent "{monpoly_log_string}" to monpoly'}
            else:
                return {'error': 'Error while logging events monpoly stdin is None'}
        else:
            return {'error': 'Monpoly is not running'}
    
def log_events(mon: Monitor, events_json: str):
    # get current time at this point, so all events with a missing timestamp are logged with the same timestamp
    timestamp_now = datetime.now()
    with open(events_json) as f:
        try:
            events = json.load(f)
            events = [{'timestamp-int': get_timestamp(e, timestamp_now)} | e for e in events]
            list.sort(events, key=lambda e: e['timestamp-int'])
            monpoly_log = send_events_to_monpoly(mon, events)
            if 'error' not in monpoly_log.keys():
                monpoly_log |= store_events_in_db(mon, events)

            return monpoly_log

        except ValueError as e:
            print('Error parsing json file: {}'.format(e))
            mon.clear_directory(mon.events_dir)
            return {'error': f'Error while parsing events JSON {e}'}
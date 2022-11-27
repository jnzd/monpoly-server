import json
import os
import subprocess
from datetime import datetime
from dateutil import parser
from dateutil.parser import ParserError
import time
import psycopg2
from questdb.ingress import Buffer, Sender
from db_helper import DbHelper

# if this path is absolute all subsequent paths are relative to this path
# will be absolute paths
CONFIG_DIR = os.path.abspath('./monitor-data/')
LOG_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

class Monitor:
    def __init__(self):
        # should the policy be negated?
        self.policy_negate = False
        # database helper object
        self.db = DbHelper()
        # directory paths
        self.signature_dir = os.path.join(CONFIG_DIR, 'signature')
        self.policy_dir = os.path.join(CONFIG_DIR, 'policies')
        self.sql_dir = os.path.join(CONFIG_DIR, 'sql')
        self.events_dir = os.path.join(CONFIG_DIR, 'events')
        self.monpoly_stdout_dir = os.path.join(CONFIG_DIR, 'monpoly-stdout')
        self.backend_data_dir = os.path.join(CONFIG_DIR, 'backend-data')
        # create directories if they don't exist
        self.make_dirs(self.signature_dir)
        self.make_dirs(self.policy_dir)
        self.make_dirs(self.sql_dir)
        self.make_dirs(self.monpoly_stdout_dir)
        self.make_dirs(self.events_dir)
        self.make_dirs(self.backend_data_dir)
        self.conf_path = os.path.join(self.backend_data_dir, 'conf.json')
        self.log_path = os.path.join(self.backend_data_dir, 'backend.log')
        self.monitor_state_path = os.path.join(self.backend_data_dir, 'monpoly_state.bin')
        # paths to individual files
        self.signature_path = os.path.join(self.signature_dir, 'signature.sig')
        self.sig_json_path = os.path.join(self.signature_dir, 'sig.json')
        self.policy_path = os.path.join(self.policy_dir, 'policy.mfotl')
        self.sql_drop_path = os.path.join(self.sql_dir, 'drop.sql')
        self.monpoly_stdout_path = os.path.join(self.monpoly_stdout_dir, 'monpoly_stdout.log')
        self.monitorability_log_path = os.path.join(self.monpoly_stdout_dir, 'monitorability.log')

        self.most_recent_timestamp = None
        # second column isn't necessary for the functionality of the backend,
        # but questdb doesn't currently (2022-11-17) support tables with only
        # timestamp column:
        # https://github.com/questdb/questdb/issues/2691
        self.ts_query_create = "CREATE TABLE ts(dummy_column BYTE,time_stamp TIMESTAMP) timestamp(time_stamp) PARTITION BY DAY;"
        self.ts_query_drop = "DROP TABLE ts;"
        self.monpoly = None
        self.restore_state()
        self.write_config()

    def write_server_log(self, msg: str):
        time_stamp = datetime.now().strftime(LOG_TIMESTAMP_FORMAT)
        with open(self.log_path, 'a') as log:
            log.write(f'[{time_stamp}] {msg}\n')

    def check_monitorability(self, sig, pol):
        self.write_server_log(f'checking monitorability of {sig} and {pol}')
        cmd = ['monpoly', '-check', '-sig', sig, '-formula', pol]
        process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        response = process.stdout
        with open(self.monitorability_log_path, 'w') as log:
            log.write(response)
        
        if "The analyzed formula is monitorable." not in response:
            return {'monitorable': False, 'message': response}
        else:
            return {'monitorable': True, 'message': response}

    def get_monitorability_log(self):
        if os.path.exists(self.monitorability_log_path):
            with open(self.monitorability_log_path, 'r') as log:
                return log.read()
        else:
            return "monitorability not yet checked"
    
    def signature_set(self):
        '''returns true if the signature is set'''
        return self.signature_path and os.path.exists(self.signature_path)

    def policy_set(self):
        '''returns true if the policy is set'''
        return self.policy_path and os.path.exists(self.policy_path)

    def make_dirs(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
    
    def db_is_empty(self) -> bool:
        '''returns true if the database is empty'''
        return not os.path.exists(self.sql_drop_path)
    
    def get_signature(self):
        if os.path.exists(self.signature_path):
            with open(self.signature_path, 'r') as sig_file:
                return sig_file.read()
        else:
            return 'no signature set'
    
    def get_json_signature(self):
        if os.path.exists(self.sig_json_path):
            with open(self.sig_json_path, 'r') as sig_json:
                return {'json': json.load(sig_json)}
        else:
            return {'error': 'json signature not set yet'}
    
    def get_policy(self):
        policy = ''
        if not os.path.exists(self.policy_path):
            policy = 'no policy set'
        else: 
            with open(self.policy_path, 'r') as pol_file:
                policy = pol_file.read()
                pol_file.close()
        return f'{"NOT" if self.policy_negate else ""} {policy}'
    
    def get_schema(self):
        return self.db.run_query('SHOW TABLES;', select = True)

    def get_config(self) -> dict:
        config = {'policy_negate': self.policy_negate,
                  'db': self.db.get_config(),
                  'most_recent_timestamp': datetime.strftime(self.most_recent_timestamp, LOG_TIMESTAMP_FORMAT) if self.most_recent_timestamp else None,
                }
        return config
    
    def restore_db(self, conf):
        if 'database' in conf.keys():
            self.db = DbHelper(conf['database'])
            self.write_server_log(f'Restored database connection: {self.db.get_config()}')
        else:
            self.db = DbHelper()
            self.write_server_log(f'established database connection: {self.db.get_config()}')
        
    def restore_state(self):
        # TODO: when get_config() gets changed, change this as well
        if os.path.exists(self.conf_path):
            with open(self.conf_path, 'r') as conf_json:
                conf = json.load(conf_json)
                self.policy_negate = conf['policy_negate']
                tp = conf['most_recent_timestamp']
                if tp is not None:
                    self.most_recent_timestamp = parser.parse(tp)
                self.restore_db(conf)
                self.write_server_log(f'[restore_state()] restored state with: {conf}')
        else:
            self.write_server_log(f'[restore_state()] config file doesn\'t exist: {self.conf_path}')
    
    def write_config(self):
        conf = self.get_config()
        with open(self.conf_path, 'w') as conf_json:
            conf_string = json.dumps(conf)
            conf_json.write(conf_string)
            self.write_server_log(f'wrote config: {conf_string}')

    def set_policy(self, policy, negate: bool=False):
        # as long as monpoly isn't running yet, the policy can still be changed
        if os.path.exists(self.policy_path) and self.monpoly:
            return {'error': f'monpoly is already running and policy has been set. Use change_policy() to change the policy.',
                    'ls pol_dir': os.listdir(self.policy_dir)}
        os.rename(policy, self.policy_path)
        self.policy_negate = negate
        self.write_server_log(f'set policy: {self.get_policy()}')
        self.write_config()
        return {'message': f'policy set to {self.get_policy()}'}

    def change_policy(self, policy, negate: bool=False, policy_change_method: str='naive'):
        if not os.path.exists(self.policy_path):
            self.write_server_log(f'[change_policy()] no policy has previously been set: {self.policy_path}')
            return {'message': f'no policy has been set previously, use /set-policy to set it',
                    'ls pol_dir': os.listdir(self.policy_dir)}

        check = self.check_monitorability(self.signature_path, self.policy_path)
        if not check['monitorable']:
            self.write_server_log('[change_policy()] cannot change policy, because policy is not monitorable')
            return {'error': check['message']}

        # TODO add check that queries for the most recent timepoint
        #      in questdb
        #      add variable storing the most recent timestamp 
        #      encountered and sent to questeb
        #      compare both values and if they are different, wait

        if self.most_recent_timestamp is not None:
            most_recent_db = self.get_most_recent_timestamp_from_db()
            while most_recent_db is None or most_recent_db < self.most_recent_timestamp:
                most_recent_db = self.get_most_recent_timestamp_from_db()
                self.write_server_log(f'[change_policy()] waiting for most recent timestamp seen to be in database: {most_recent_db} < {self.most_recent_timestamp}')
                time.sleep(10)

        old_policy = self.get_policy()
        os.rename(policy, self.policy_path)
        self.policy_negate = negate
        # update negation in config
        self.write_config()
        self.write_server_log(f'[change_policy()] changed policy from {old_policy} to {self.get_policy()}')
        timepoints = self.get_events()
        timepoints_monpoly = os.path.join(self.events_dir, 'events_policy_change.log')
        self.create_log_strings(timepoints, output_file=timepoints_monpoly)
        self.stop_monpoly(save_state=False)
        if timepoints == []:
            self.write_server_log(f'[change_policy()] no timepoints found, starting monpoly without reading old timepoints')
            self.monpoly = self.start_monpoly(self.signature_path, self.policy_path)
            self.write_server_log(f'[change_policy()] started monpoly')
        else:
            self.write_server_log(f'[change_policy()] running monpoly and reading all past timepoints')
            self.monpoly = self.start_monpoly(self.signature_path, self.policy_path, log=timepoints_monpoly)
            self.write_server_log(f'[change_policy()] started monpoly')
            if self.monpoly.stdout is None:
                return {'error': 'monpoly stdout is None'}
            output = ''
            while '## Done with log file - waiting for stdin ##' not in output:
                self.write_server_log(f'[change_policy()] waiting for monpoly to finish')
                output += self.monpoly.stdout.readline()
        self.clear_directory(self.events_dir)
        self.write_monpoly_log(f'--- policy changed from {old_policy} to {self.get_policy()} ---'.replace('\n', ''))
        self.write_monpoly_log('\n')
        return {'success': f'changed policy from {old_policy} to {self.get_policy()}'}

    def set_signature(self, sig):
        # as long as monpoly isn't running yet, the policy can still be changed
        if os.path.exists(self.signature_path):
            if self.monpoly:
                return {'error': f'signature has already been set',
                        'ls sig_dir': os.listdir(self.signature_dir)}
            else:
                self.delete_database()
        os.rename(sig, self.signature_path)
        self.init_database(self.signature_path)
        self.set_destruct_query(self.signature_path)
        self.create_json_signature(self.signature_path)
        self.write_server_log(f'set signature: {self.get_signature()}')
        self.write_config()
        return {'message': f'signature set to {self.get_signature()}'}
    
    def create_json_signature(self, sig):
        cmd = ['monpoly', '-sig_to_json', sig]
        with open(self.sig_json_path, 'w') as json_sig:
            process = subprocess.run(cmd, capture_output=True, text=True)
            if process.stderr:
                return {'error': f'create_json_signature: {process.stderr}'}                                
            json_sig.write(process.stdout)

        return self.get_json_signature()

    def set_destruct_query(self, sig):
        cmd = ['monpoly', '-sql_drop', sig]
        process = subprocess.run(cmd, capture_output=True, text=True)
        query_drop = process.stdout
        query_drop += self.ts_query_drop
        self.write_server_log(f'[get_destruct_query()] Generated drop query: {query_drop}')
        with open(self.sql_drop_path, 'w') as drop_file:
            drop_file.write(query_drop)
        return {'drop query': query_drop, 'drop file': self.sql_drop_path}

    def init_database(self, sig, verbose: bool = True):
        '''
        Creates a database from the given signature file
        '''
        if verbose: print(f'Creating database')
        cmd = ['monpoly', '-sql', sig]
        process = subprocess.run(cmd, capture_output=True, text=True)
        query_create = process.stdout
        self.db.run_query(query_create)
        self.db.run_query(self.ts_query_create)
        return {'created tables': query_create + self.ts_query_create}

    def start_monpoly(self, sig, pol, restart: str='', log: str=''):
        cmd = ['monpoly', 
               '-unix',
               '-ack_sep',
               '-ignore_parse_errors',
               '-tolerate_faulty_predicates',
               '-sig', sig,
               '-formula', pol
               ]
        if restart:
            cmd.append('-load')
            cmd.append(restart)

        if self.policy_negate:
            cmd.append('-negate')

        if log != '':
            cmd.append('-log')
            cmd.append(log)
            cmd.append('-switch_to_stdin_after_log')
            cmd.append('-suppress_stdout')
            cmd.append('-nonewlastts')
        
        self.write_server_log(f'[spawn_monpoly()] cmd={cmd}')
        p = subprocess.Popen(cmd,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            text=True,
                            start_new_session=True)
        if not p.stdout:
            self.write_server_log(f'[spawn_monpoly()] monpoly_process.stdout is None')
        return p

    def launch(self, restart=False):
        '''
        starts or restarts monpoly and returns a string message
        '''
        if self.monpoly and self.monpoly.poll() is None:
            self.write_server_log(f'[launch()] monpoly already running, self.monpoly.poll(): {self.monpoly.poll()}')
            return 'monpoly not started, because it is already running'
        self.write_server_log('[launch()] launching monpoly')
        if not self.signature_set():
            self.write_server_log('[launch()] cannot launch monpoly, because signature is not set')
            return 'no signature provided'
        elif not self.policy_set():
            self.write_server_log('[launch()] cannot launch monpoly, because policy is not set')
            return 'no policy provided'

        if not restart:
            check = self.check_monitorability(self.signature_path, self.policy_path)
            if not check['monitorable']:
                self.write_server_log('[launch()] cannot launch monpoly, because policy is not monitorable')
                return check['message']

        if os.path.exists(self.monitor_state_path):
            self.write_server_log(f'[launch()] attempting to restart monpoly and load state from: {self.monitor_state_path}')
            self.monpoly = self.start_monpoly(self.signature_path, self.policy_path, restart=self.monitor_state_path)
            return 'restarted monpoly'
            
        if not restart:
            self.monpoly = self.start_monpoly(self.signature_path, self.policy_path)
            self.write_server_log('launched monpoly')
            return f'successfully launched monpoly, pid: {self.get_monpoly_pid()}, args: {self.monpoly.args}'
        else:
            return f'cannot restart monpoly, because it was not previously started'

    def delete_database(self):
        '''
        Deletes the database associated with the given signature file
        '''
        query = ''
        if not self.db_is_empty():
            with open(self.sql_drop_path, 'r') as drop_file:
                query = drop_file.read()
                drop_file.close()
        elif self.db_is_empty():
            self.write_server_log(f'delete_database(): database is already empty (os.listdir({self.sql_dir}): {os.listdir(self.sql_dir)})')
            return {'error': 'Database is already empty'}

        self.write_server_log(f'delete_database(): deleting tables associated with {self.get_signature()}')

        # TODO prompt user before running this query and deleting all tables
        self.db.run_query(query)
        os.remove(self.sql_drop_path)
        return{'query': query}
    
    def clear_directory(self, path):
        self.write_server_log(f'clearing directory: {path}')
        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))

    def delete_config(self):
        if os.path.exists(self.conf_path):
            os.remove(self.conf_path)
        return {'config': f'deleted {self.conf_path}'}

    def delete_everything(self):
        stop_log = self.stop_monpoly(save_state=False)
        drop_log = self.delete_database()
        conf_log = self.delete_config()
        self.clear_directory(self.signature_dir)
        self.clear_directory(self.policy_dir)
        self.clear_directory(self.events_dir)
        self.clear_directory(self.monpoly_stdout_dir)
        self.clear_directory(self.sql_dir)
        if os.path.exists(self.monitor_state_path):
            os.remove(self.monitor_state_path)
        return {'deleted everything': 'done'} | drop_log | stop_log | conf_log
    
    def stop_monpoly(self, save_state: bool = True):
        self.write_server_log('[stop()] stopping monpoly')
        log = dict()
        if not self.monpoly or self.monpoly.poll():
            self.write_server_log(f'[stop()] monpoly is not running, self.monpoly: {self.monpoly}')
            return {'error': 'monpoly not running or already stopped'}

        if self.monpoly and self.monpoly.poll() is None:
            if save_state and self.monpoly.stdin:
                self.write_server_log(f'[stop()] sending > save_and_exit {self.monitor_state_path} <; to monpoly')
                self.monpoly.stdin.write(f'> save_and_exit {self.monitor_state_path} < ;')
                self.monpoly.stdin.flush()
                self.write_server_log('[stop()] waiting for response from monpoly')
                return_code = self.monpoly.wait()
                self.write_server_log(f'[stop()] monpoly exited with return code: {return_code}, self.monpoly.poll(): {self.monpoly.poll()}, saved state at {self.monitor_state_path}')
                log |= {'stopped monpoly and stored sate, return code': return_code}
            elif not save_state:
                self.write_server_log("[stop()] stopping monpoly without saving state")
                self.monpoly.kill()
            else:
                self.write_server_log("[stop()] can't access stind of monpoly, stopping without saving state")
                self.monpoly.kill()

        return {'stopped': 'stopped monpoly'} | log

    def get_monpoly_pid(self):
        if self.monpoly:
            return self.monpoly.pid
        else:
            return f'monpoly not running'
    
    def get_monpoly_exit_code(self):
        if self.monpoly:
            exit_code = self.monpoly.poll()
            if exit_code is not None:
                return exit_code
            else:
                return 'monpoly still running'
        else:
            return 'monpoly not running (yet)'

    def write_monpoly_log(self, log):
        with open(self.monpoly_stdout_path, 'a') as monpoly_log:
            monpoly_log.write(log)

    def get_stdout(self):
        if not os.path.exists(self.monpoly_stdout_path):
            return 'error stdout log does not exist'
        with open(self.monpoly_stdout_path, 'r') as stdout:
            return stdout.read() or 'stdout is empty'

    def get_most_recent_timestamp_from_db(self):
        # TODO alternative to try/except is checking if dv is empty first
        try:
            t = self.db.run_query('SELECT MAX(time_stamp) FROM ts;', select=True)
            if t:
                return t[0][0]
            else:
                return None
        except psycopg2.DatabaseError:
            return None

    def store_timepoints_in_db(self, timepoints: list):
        ''' logs the given events in the database '''
        buf = Buffer()
        for timepoint in timepoints:
            if 'skip' in timepoint.keys():
                continue
            ts = datetime.fromtimestamp(timepoint['timestamp-int'])
            # dummy_column is necessary, because questdb doesn't support tables
            # with only one timestamp column (in combination with the influxDB Line Protocol)
            # https://github.com/questdb/questdb/issues/2691
            buf.row('ts', symbols=None, columns={'dummy_column':0}, at=ts)
            for p in timepoint['predicates']:
                if 'name' not in p.keys():
                    return {'log_events error': 'predicate must have a "name"'}
                elif 'occurrences' not in p.keys():
                    # predicate can be named without an occurrence
                    break
                name = p['name']
                for occ in p['occurrences']:
                    columns = {'dummy_column': 0} | {f'x{i+1}': o for i,o in enumerate(occ)}
                    buf.row( name, symbols = None, columns = columns, at = ts)
                self.most_recent_timestamp = ts
        # update config after going over all timestamps
        self.write_config()
        with Sender(self.db.host, self.db.port_influxdb) as sender:
            self.write_server_log(f'sending buffer {buf} to database')
            sender.flush(buf)

        return {'events': timepoints}

    def send_timepoint_to_monpoly(self, event_str: str):
        if self.monpoly:
            if self.monpoly.stdin and self.monpoly.stdout:
                self.write_server_log(f'[send_events_to_monpoly({event_str})] sending events to monpoly: {event_str}')
                self.monpoly.stdin.write(event_str)
                self.monpoly.stdin.flush()
                result = ''
                reached_separator = False
                while not reached_separator:
                    line = self.monpoly.stdout.readline()
                    self.write_server_log(f'[send_events_to_monpoly({event_str})] read line from monpoly: {line}')
                    reached_separator = '## reached separator ##' in line
                    if not reached_separator:
                        result += line

                self.write_monpoly_log(result)
                self.write_server_log(f'[send_events_to_monpoly({event_str})] monpoly done - stdout: {result}')
                return{'success': f'sent "{event_str}" to monpoly', 'output': result}
            else:
                self.write_server_log(f'could not access stdin or stdout of monpoly (stdout:{self.monpoly.stdout}, stdin:{self.monpoly.stdin})')
                return {'error': 'Error while logging events monpoly stdin is None'}
        else:
            self.write_server_log('error: monpoly is not running')
            return {'error': 'Monpoly is not running'}

            
    def create_log_strings(self, timepoints: list, output_file=None):
        '''
        this function takes a list of event dictionaries
        it adds log strings (to be sent to monpoly) to for 
        each timestamp and returns the extended list of 
        dictionaries
        '''
        self.write_server_log(f'create_log_strings({timepoints})')
        for timepoint in timepoints:
            timestamp = timepoint['timestamp-int']
            monpoly_string = f'@{timestamp} '

            for predicate in timepoint['predicates']:
                if 'name' not in predicate.keys():
                    timepoint['skip'] = f'predicate {predicate} has no name'
                    self.write_server_log(f'create_log_strings(): predicate ({predicate}) with no name at timestamp: {timestamp}')
                    break
                else:
                    name = predicate['name']
                    for occurrence in predicate['occurrences']:
                        predicate_str = f'{name} {self.tuple_str_from_list(occurrence)} '
                        monpoly_string += predicate_str
            monpoly_string += ';\n'
            timepoint['monpoly-string'] = monpoly_string
            if output_file is not None:
                with open(output_file, 'a') as f:
                    f.write(monpoly_string)

            self.write_server_log(f'create_log_strings(): created monpoly string: {monpoly_string}')
        return timepoints

    def tuple_str_from_list(self, l: list) -> str:
        l_str = [str(x) for x in l]
        return '(' + ', '.join(l_str) + ')'
    
    def get_timestamp(self, event: dict, timestamp_now: datetime) -> int:
        '''
        This method checks if the event has a timestamp
        it sets it to the current time if it doesn't
        It returns a timestamp in seconds since 1970-01-01 00:00:00 
        (in monpoly/scr/formula_parser.mly:timeunits it can be seen that 
        seconds are the smallest and default time unit in monpoly)
        '''
        if 'timestamp' in event.keys():
            try:
                ts = parser.parse(event['timestamp'])
            except ParserError:
                # TODO: is this desirable or should this timepoint be ignored and skipped?
                # could return -1 and later check if 'timestamp-int' is -1
                ts = timestamp_now
        else:
            ts = timestamp_now
        return int(ts.timestamp())
    
    def log_timepoints(self, timepoints_json: str):
        # get current time at this point, so all events with a missing timestamp are logged with the same timestamp
        self.write_server_log(f'[log_timepoints()] started logging events: {timepoints_json}')
        timestamp_now = datetime.now()
        with open(timepoints_json) as f:
            try:
                timepoints = json.load(f)
                timepoints = [{'timestamp-int': self.get_timestamp(e, timestamp_now)} | e for e in timepoints]
                list.sort(timepoints, key=lambda e: e['timestamp-int'])
                timepoints = self.create_log_strings(timepoints)

                skip_log = {}
                for timepoint in timepoints:
                    if 'skip' in timepoint.keys():
                        self.write_server_log(f'[log_timepoints()] skipping event: {timepoint}, because: {timepoint["skip"]}')
                        skip_log |= {timepoint['timestamp-int']: timepoint['skip']}
                        continue
                    monpoly_output = self.send_timepoint_to_monpoly(timepoint['monpoly-string'])
                    if 'error' in monpoly_output.keys():
                        return {'error': f'error while logging timepoints: {monpoly_output["error"]}'}
                    output = monpoly_output['output']
                    if 'WARNING: Skipping out of order timestamp' in output or 'ERROR' in output:
                        timepoint['skip'] = output
                        skip_log |= {timepoint['timestamp-int']: timepoint['skip']}
                db_response = self.store_timepoints_in_db(timepoints)
                self.write_server_log(f'stored events in db: {db_response}')

                return { 'skipped-timepoints': skip_log}
                # return {'db-response': db_response,
                #         'skipped-timepoints': skip_log}

            except ValueError as e:
                print(f'error parsing json file: {e}')
                self.write_server_log(f'error parsing json file: {e}')
                self.clear_directory(self.events_dir)
                return {'error': f'Error while parsing events JSON {e}'}

    def db_response_to_timepoints(self, db_response: list) -> list:
        self.write_server_log(f'[db_response_to_timepoints()] converting db response to timepoints')
        db_response_dict = {k: v for d in db_response for k, v in d.items()}
        timestamps = {x[1] for x in db_response_dict['ts']}
        result = dict()
        for ts in timestamps:
            ts_int = int(ts.timestamp())
            ts_dict = {'timestamp-int': ts_int, 'timestamp': ts.strftime(LOG_TIMESTAMP_FORMAT), 'predicates': dict()}
            result[ts_int] = ts_dict

        for predicate_name in db_response_dict.keys():
            if predicate_name == 'ts':
                continue
            for occurrence in db_response_dict[predicate_name]:
                ts = int(occurrence[-1].timestamp())
                # the first value in `occurence` is from `dummy_column`
                # the last value in `occurence` is the timestamp
                if predicate_name in result[ts]['predicates'].keys():
                    result[ts]['predicates'][predicate_name].append(occurrence[1:-1])
                else:
                    result[ts]['predicates'][predicate_name] = [occurrence[1:-1]]

        result = [v for _, v in result.items()]
        for t in result:
            t['predicates'] = [{'name': k, 'occurrences': v} for k, v in t['predicates'].items()]
        result.sort(key=lambda e: e['timestamp-int'])

        return result
        
    def get_events(self, start_date=None, end_date=None) -> list:
        '''
        returns all events in the database in the same json format
        that this backend takes as input
        '''
        names = []
        if os.path.exists(self.sig_json_path):
            with open(self.sig_json_path) as f:
                signature = json.load(f)
                for predicate in signature:
                    names.append(predicate['name'])

        if start_date is not None and end_date is not None:
            # BETWEEN is inclusive
            query_suffix = f"WHERE time_stamp BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date is not None:
            query_suffix = f"WHERE time_stamp >= '{start_date}'"
        elif end_date is not None:
            query_suffix = f"WHERE time_stamp <= '{end_date}'"
        else:
            query_suffix = ''

        names.append('ts')
        results = []
        for table_name in names:
            self.write_server_log(f'[get_events()] getting events for table: {table_name}')
            response = self.db.run_query(f'SELECT * FROM {table_name} {query_suffix};', select=True)
            self.write_server_log(f'[get_events()] got response from db: {response}')
            results.append({table_name: response})
        monpoly_log = self.db_response_to_timepoints(results)
        return monpoly_log

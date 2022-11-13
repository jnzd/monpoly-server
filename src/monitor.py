from db_helper import DbHelper
import os
import subprocess
import json
from datetime import datetime
import log_events
from questdb.ingress import Sender
from questdb.ingress import Buffer

config_dir       = os.path.abspath('./monitor-data/')
signature_dir    = f'{config_dir}/signature'
policy_dir       = f'{config_dir}/policies'
sql_dir          = f'{config_dir}/sql'
events_dir       = f'{config_dir}/events'
monitor_logs_dir = f'{config_dir}/monitor-logs'


class Monitor:
    def __init__(self):
        self.sig = ''
        self.policy = ''
        self.policy_negate = False
        self.conf_path = f'{config_dir}/conf.json'
        self.log_path = f'{config_dir}/log.txt'
        self.db = DbHelper()
        self.sig_dir = os.path.abspath(signature_dir)
        self.pol_dir = os.path.abspath(policy_dir)
        self.sql_dir = os.path.abspath(sql_dir)
        self.sql_drop = f'{self.sql_dir}/drop.sql'
        self.events_dir = os.path.abspath(events_dir)
        self.monitor_logs = os.path.abspath(monitor_logs_dir)
        self.monitor_state_path = f'{self.monitor_logs}/state.txt'
        self.sig_json_path = f'{self.sig_dir}/sig.json'
        self.monpoly_log = f'{self.monitor_logs}/monpoly_stdout.log'
        self.monitorability_log = f'{self.monitor_logs}/monitorability.log'
        self.make_dirs(self.sig_dir)
        self.make_dirs(self.pol_dir)
        self.make_dirs(self.sql_dir)
        self.make_dirs(self.monitor_logs)
        self.make_dirs(self.events_dir)
        self.monpoly = None
        self.restore_state()
        self.write_config()

    def write_server_log(self, msg: str):
        time_stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_path, 'a') as log:
            log.write(f'[{time_stamp}] {msg}\n')

    def check_monitorability(self, sig, pol):
        self.write_server_log(f'checking monitorability of {sig} and {pol}')
        cmd = ['monpoly', '-check', '-sig', sig, '-formula', pol]
        process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        response = process.stdout
        with open(self.monitorability_log, 'w') as log:
            log.write(response)
        
        if "The analyzed formula is monitorable." not in response:
            return {'monitorable': False, 'message': response}
        else:
            return {'monitorable': True, 'message': response}

    def get_monitorability_log(self):
        if os.path.exists(self.monitorability_log):
            with open(self.monitorability_log, 'r') as log:
                return log.read()
        else:
            return "monitorability not yet checked"
    
    def signature_set(self):
        '''returns true if the signature is set'''
        return self.sig and os.path.exists(self.sig)

    def policy_set(self):
        '''returns true if the policy is set'''
        return self.policy and os.path.exists(self.policy)

    def make_dirs(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
    
    def db_is_empty(self) -> bool:
        '''returns true if the database is empty'''
        return not os.path.exists(self.sql_drop)
    
    def get_signature(self):
        if os.path.exists(self.sig):
            with open(self.sig, 'r') as sig_file:
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
        if not os.path.exists(self.policy):
            policy = 'no policy set'
        else: 
            with open(self.policy, 'r') as pol_file:
                policy = pol_file.read()
                pol_file.close()
        return policy
    
    def get_schema(self):
        return self.db.run_query('SHOW TABLES;', select = True)

    def get_config(self) -> dict:

        config = {'signature': self.sig,
                  'signature_json': self.sig_json_path,
                  'policy': self.policy,
                  'policy_negate': self.policy_negate,
                  'db': self.db.get_config(),
                  'sql_drop': self.sql_drop
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
        self.write_server_log(f'restore_state()')
        if os.path.exists(self.conf_path):
            self.write_server_log(f'[restore_state()] config file exists: {self.conf_path}')
            with open(self.conf_path, 'r') as conf_json:
                conf = json.load(conf_json)
                self.write_server_log(f'[restore_state()] config file loaded: {conf}')
                self.sig = conf['signature']
                self.sig_json = conf['signature_json']
                self.policy = conf['policy']
                self.policy_negate = bool(conf['policy_negate'])

                self.write_server_log(f'[restore_state()] calling restore_db({conf})')
                self.restore_db(conf)
    
    def write_config(self):
        conf = self.get_config()
        with open(self.conf_path, 'w') as conf_json:
            conf_string = json.dumps(conf)
            conf_json.write(conf_string)
            self.write_server_log(f'wrote config: {conf_string}')

    def set_policy(self, policy, negate: bool=False):
        policy_location = f'{self.pol_dir}/policy'
        # as long as monpoly isn't running yet, the policy can still be changed
        # TODO allow for policy change later on
        if os.path.exists(policy_location) and self.monpoly:
            return {'error': f'policy has already been set',
                    'policy_location': policy_location,
                    'ls pol_dir': os.listdir(self.pol_dir)}
        policy_location_abs = os.path.abspath(policy_location)
        os.rename(policy, policy_location_abs)
        self.policy = policy_location_abs
        self.policy_negate = negate
        self.write_server_log(f'set policy: {policy_location_abs}')
        self.write_config()
        return {'set policy': policy}

    def set_signature(self, sig):
        sig_location = f'{self.sig_dir}/sig'
        # as long as monpoly isn't running yet, the policy can still be changed
        if os.path.exists(sig_location):
            if self.monpoly:
                return {'error': f'signature has already been set',
                        'sig_location': sig_location,
                        'ls sig_dir': os.listdir(self.sig_dir)}
            else:
                self.delete_database()
        sig_location_abs = os.path.abspath(sig_location)
        os.rename(sig, sig_location_abs)
        self.sig = sig_location_abs
        init_log = self.init_database(self.sig)
        drop_log = self.get_destruct_query(self.sig)
        json_log = self.create_json_signature(self.sig)
        self.write_server_log(f'set signature: {sig_location_abs}')
        self.write_config()
        return init_log | drop_log | json_log
    
    def create_json_signature(self, sig):
        cmd = ['monpoly', '-sig_to_json', sig]
        with open(self.sig_json_path, 'w') as json_sig:
            process = subprocess.run(cmd, capture_output=True, text=True)
            if process.stderr:
                return {'error': f'create_json_signature: {process.stderr}'}                                
            json_sig.write(process.stdout)

        return self.get_json_signature()
        

    def get_destruct_query(self, sig, verbose: bool = True):
        cmd = ['monpoly', '-sql_drop', sig]
        process = subprocess.run(cmd, capture_output=True, text=True)
        query_drop = process.stdout

        if verbose: print(f'Generated drop query: {query_drop}')

        self.sql_drop_tables = query_drop
        location = self.sql_drop

        if verbose: print(f'Writing drop query to {location}')

        with open(location, 'w') as drop_file:
            drop_file.write(query_drop)
            drop_file.close()
        return {'drop query': query_drop, 'drop file': location}

    def init_database(self, sig, verbose: bool = True):
        '''
        Creates a database from the given signature file
        '''
        if verbose: print(f'Creating database')
        cmd = ['monpoly', '-sql', sig]
        process = subprocess.run(cmd, capture_output=True, text=True)
        query_create = process.stdout
        self.db.run_query(query_create)
        return {'created tables': query_create}

    def spawn_monpoly(self, sig, pol, restart: str=''):
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
        self.write_server_log(f'[spawn_monpoly()] cmd={cmd}')
        p = subprocess.Popen(cmd,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            text=True)
        if not p.stdout:
            self.write_server_log(f'[spawn_monpoly()] monpoly_process.stdout is None')
        return p

    def launch(self):
        '''
        starts or restarts monpoly and returns a string message
        '''
        if self.monpoly and self.monpoly.poll() is None:
            self.write_server_log(f'[launch()] monpoly already running, self.monpoly.poll(): {self.monpoly.poll()}')
            return 'monpoly not started, because it is already running'
        self.write_server_log('[launch()] launching monpoly')
        if not self.signature_set():
        # if not self.sig:
            self.write_server_log('[launch()] cannot launch monpoly, because signature is not set')
            return 'no signature provided'
        elif not self.policy_set():
        # elif not self.policy:
            self.write_server_log('[launch()] cannot launch monpoly, because policy is not set')
            return 'no policy provided'

        check = self.check_monitorability(self.sig, self.policy)
        if not check['monitorable']:
            self.write_server_log('[launch()] cannot launch monpoly, because policy is not monitorable')
            return check['message']

        if os.path.exists(self.monitor_state_path):
            self.write_server_log(f'[launch()] attempting to restart monpoly and load state from: {self.monitor_state_path}')
            self.monpoly = self.spawn_monpoly(self.sig, self.policy, restart=self.monitor_state_path)
            return 'restarted monpoly'
            
        # self.init_database(self.sig)
        self.monpoly = self.spawn_monpoly(self.sig, self.policy)
        self.write_server_log('launched monpoly')
        return f'successfully launched monpoly, pid: {self.get_monpoly_pid()}, args: {self.monpoly.args}'

    def delete_database(self):
        '''
        Deletes the database associated with the given signature file
        '''
        query = ''
        if not self.db_is_empty():
            with open(f'{self.sql_dir}/drop.sql', 'r') as drop_file:
                query = drop_file.read()
                drop_file.close()
        elif self.db_is_empty():
            self.write_server_log(f'delete_database(): database is already empty (os.listdir({self.sql_dir}): {os.listdir(self.sql_dir)})')
            return {'error': 'Database is already empty'}

        self.write_server_log(f'delete_database(): deleting tables associated with {self.sig}')
        self.write_server_log(f'delete_database(): running query: {query}')

        # TODO prompt user before running this query and deleting all tables
        self.db.run_query(query)
        os.remove(f'{self.sql_dir}/drop.sql')
        return{'query': query}
    
    def clear_directory(self, path):
        self.write_server_log(f'clearing directory: {path}')
        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))

    def delete_everything(self):
        stop_log = self.stop()
        drop_log = self.delete_database()
        self.clear_directory(self.sig_dir)
        self.clear_directory(self.pol_dir)
        self.clear_directory(self.events_dir)
        self.clear_directory(self.monitor_logs)
        return {'deleted everything': 'done'} | drop_log | stop_log
    
    def stop(self):
        self.write_server_log('[stop()] stopping monpoly')
        log = dict()
        if not self.monpoly or self.monpoly.poll():
            self.write_server_log('[stop()] monpoly is not running')
            return {'error': 'monpoly not running or already stopped'}

        if self.monpoly:
            if self.monpoly.stdin:
                self.write_server_log(f'[stop()] sending > save_and_exit {self.monitor_state_path} <; to monpoly')
                self.monpoly.stdin.write(f'> save_and_exit {self.monitor_state_path} < ;')
                self.monpoly.stdin.flush()
                self.write_server_log('[stop()] waiting for response from monpoly')
                return_code = self.monpoly.wait()
                self.write_server_log(f'[stop()] monpoly exited with return code: {return_code}, self.monpoly.poll(): {self.monpoly.poll()}, saved state at {self.monitor_state_path}')
                log |= {'stopped monpoly and stored sate, return code': return_code}
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
        with open(self.monpoly_log, 'a') as monpoly_log:
            monpoly_log.write(log)

    def get_stdout(self):
        if not os.path.exists(self.monpoly_log):
            return {'error': 'stdout log does not exist'}
        with open(self.monpoly_log, 'r') as stdout:
            return stdout.read() or 'stdout is empty'


    def store_timepoints_in_db(self, timepoints: list):
        '''
        logs the given events in the database
        '''
        buf = Buffer()
        for timepoint in timepoints:
            if 'skip' in timepoint.keys():
                continue
            ts = datetime.fromtimestamp(timepoint['timestamp-int'])
            for p in timepoint['predicates']:
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
                    self.write_server_log(f'store_events_in_db(): added row to buffer: {columns} at {ts}')
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
                    self.write_server_log(f'[send_events_to_monpoly({event_str})] reading monpoly response')
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

            
    def create_log_strings(self, timepoints: list) -> list:
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
            self.write_server_log(f'create_log_strings(): created monpoly string: {monpoly_string}')
        return timepoints

    def tuple_str_from_list(self, l: list) -> str:
        l_str = [str(x) for x in l]
        return '(' + ', '.join(l_str) + ')'
    
    def log_timepoints(self, timepoints_json: str):
        # get current time at this point, so all events with a missing timestamp are logged with the same timestamp
        self.write_server_log(f'[log_timepoints()] started logging events: {timepoints_json}')
        timestamp_now = datetime.now()
        with open(timepoints_json) as f:
            try:
                timepoints = json.load(f)
                timepoints = [{'timestamp-int': log_events.get_timestamp(e, timestamp_now)} | e for e in timepoints]
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
                        #TODO pass along to user that this timestamp was skipped
                        # and make sure it isn't logged in the database
                        timepoint['skip'] = output
                        skip_log |= {timepoint['timestamp-int']: timepoint['skip']}

                
                    
                db_response = self.store_timepoints_in_db(timepoints)
                self.write_server_log(f'stored events in db: {db_response}')

                return {'success': 'logged events',
                        'db_response': db_response,
                        'skipped timepoints': skip_log}

            except ValueError as e:
                print(f'error parsing json file: {e}')
                self.write_server_log(f'error parsing json file: {e}')
                self.clear_directory(self.events_dir)
                return {'error': f'Error while parsing events JSON {e}'}


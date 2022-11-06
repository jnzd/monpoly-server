from db_helper import DbHelper
import os
import subprocess
import json
from datetime import datetime

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
                    

    def make_dirs(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
    
    def db_is_empty(self) -> bool:
        return not os.path.exists(f'{self.sql_dir}drop.sql')
    
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

    def restart_monpoly(self, monpoly_state):
        cmd = ['monpoly', 
               '-sig', f'{self.sig}',
               '-formula', f'{self.policy}',
               '-load', f'{monpoly_state}']
        monpoly_process = subprocess.Popen(cmd,
                                           stdin=subprocess.PIPE,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.STDOUT,
                                           text=True)
        self.write_server_log(f'restarted monpoly with cmd: {cmd} (pid: {monpoly_process.pid})')
        return monpoly_process
    
    def get_config(self) -> dict:

        config = {'signature': self.sig,
                  'signature_json': self.sig_json_path,
                  'policy': self.policy,
                  'policy_negate': self.policy_negate,
                  'db': self.db.get_config(),
                  'sql_drop': self.sql_drop
                }
        return config
    
    def restore_state(self):
        if os.path.exists(self.conf_path):
            with open(self.conf_path, 'r') as conf_json:
                conf = json.load(conf_json)
                self.sig = conf['signature']
                self.sig_json = conf['signature_json']
                self.policy = conf['policy']
                self.policy_negate = bool(conf['policy_negate'])

                if 'database' in conf.keys():
                    self.db = DbHelper(conf['database'])
                    self.write_server_log(f'Restored database connection: {self.db.get_config()}')
                else:
                    self.db = DbHelper()
                    self.write_server_log(f'established database connection: {self.db.get_config()}')

                if 'monpoly_started' in conf.keys() and bool(conf['monpoly_started']):
                    if not self.sig:
                        self.write_server_log('faulty config, cannot restart monpoly, because signature is not set')
                    if not self.policy:
                        self.write_server_log('faulty config, cannot restart monpoly, because policy is not set')
                        
                    if 'monpoly_state' in conf.keys():
                        monpoly_state = conf['monpoly_state']
                        abs_path = os.path.abspath(monpoly_state)
                        self.monpoly = self.restart_monpoly(abs_path)
                    else:
                        #TODO start new monpoly process and rerun all events
                        pass
                
    
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
        # cmd_drop = f'monpoly -sql_drop {sig}'
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

    def spawn_monpoly(self, sig, pol):
        cmd = ['monpoly', 
               '-unix',
               '-ignore_parse_errors',
               '-ack_sep',
               '-sig', sig,
               '-formula', pol
            #    '-verbose', '-debug', 'eval', '-log', './examples/logs/ex.log',
               ]
        if self.policy_negate:
            cmd.append('-negate')
        p = subprocess.Popen(cmd,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            text=True)
        return p

    def launch(self):
        self.write_server_log('launching monpoly')
        if not self.sig:
            self.write_server_log('cannot launch monpoly, because signature is not set')
            return 'no signature provided'
        elif not self.policy:
            self.write_server_log('cannot launch monpoly, because policy is not set')
            return 'no policy provided'

        check = self.check_monitorability(self.sig, self.policy)
        if not check['monitorable']:
            self.write_server_log('cannot launch monpoly, because policy is not monitorable')
            return check['message']

        save_state_path = f'{self.monitor_logs}/monitor_state'
        if os.path.exists(save_state_path):
            self.monpoly = self.restart_monpoly(save_state_path)
            return 'restarted monpoly'
            
        # self.init_database(self.sig)
        spawn_response = self.spawn_monpoly(self.sig, self.policy)
        self.monpoly = spawn_response
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
            print("""ERROR: There are no tables in the database.\nNothing to delete""")
            return {'error': 'There are no tables in the database. Nothing to delete',
                    'os.path.exists': os.path.exists('./sql/drop.sql'),
                    'ls': os.listdir(self.sql_dir)}

        
        print(f'Deleting tables associated with {self.sig}')

        # TODO prompt user before running this query and deleting all tables
        self.db.run_query(query)
        os.remove(f'{self.sql_dir}/drop.sql')
        return{'query ran': query}
    
    def clear_directory(self, path):
        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))

    def delete_everything(self):
        self.stop()
        drop_log = self.delete_database()
        self.clear_directory(self.sig_dir)
        self.clear_directory(self.pol_dir)
        self.clear_directory(self.events_dir)
        self.clear_directory(self.monitor_logs)
        return {'deleted everything': 'done'} | drop_log
    
    def stop(self):
        self.write_server_log('stopping monpoly')
        # TODO store state
        log = dict()
        if not self.monpoly or self.monpoly.poll():
            self.write_server_log('(monitor.stop) monpoly is not running')
            return {'error': 'monpoly not running or already stopped'}

        if self.monpoly:
            if self.monpoly.stdin:
                self.write_server_log(f'sending > save_and_exit {self.monitor_logs}/monitor_state <; to monpoly')
                self.monpoly.stdin.write(f'> save_and_exit {self.monitor_logs}/monitor_state < ;')
                self.monpoly.stdin.flush()
                self.write_server_log('waiting for response from monpoly')
                return_code = self.monpoly.wait()
                log |= {'stopped monpoly and stored sate, return code': return_code}
            else:
                self.write_server_log("can't access stind of monpoly, stopping without saving state")
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
            if exit_code:
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

from db_helper import DbHelper
import os
import subprocess
from pathlib import Path
import run_command

class Monitor:
    def __init__(self,
                 sig='', 
                 policy='', 
                #  directory='./monitor-data',
                 db: DbHelper = DbHelper()):
        self.sig = ''
        self.policy = ''
        self.db = db
        directory = './monitor-data'
        dir_abs = os.path.abspath(directory)
        self.sig_dir = f'{dir_abs}/signatures/'
        self.pol_dir = f'{dir_abs}/policies/'
        self.sql_dir = f'{dir_abs}/sql/'
        self.events_dir = f'{dir_abs}/events/'
        self.monitor_logs = f'{dir_abs}/monitor-logs/'
        self.make_dirs(self.sig_dir)
        self.make_dirs(self.pol_dir)
        self.make_dirs(self.sql_dir)
        self.make_dirs(self.monitor_logs)
        self.make_dirs(self.events_dir)
        self.monpoly = None
        if sig: self.set_signature(sig)
        if policy: self.set_policy(policy)

    def check_monitorability(self, sig, pol):
        cmd_check = f'monpoly -check -sig {sig} -formula {pol}'
        # TODO: run monpoly as a subprocess
        # return an object to which events can be passed to via stdin
        #TODO is `with open() as ...` the right way to do this?
        stdout_check_path = f'{self.monitor_logs}monpoly_stdout_check.log'
        stderr_check_path = f'{self.monitor_logs}monpoly_stderr_check.log'
        out = ''
        with open(stdout_check_path, 'w') as stdout_check:
            with open(stderr_check_path, 'w') as stderr_check:
                check_process = subprocess.Popen([cmd_check],
                                    stdout=stdout_check,
                                    stderr=stderr_check,
                                    text=True,
                                    shell=True)
                check_process.wait()

        with open(stdout_check_path, 'r') as stdout_check:
            with open(stderr_check_path, 'r') as stderr_check:
                out = stdout_check.read()
                err = stderr_check.read()
        
        os.remove(stdout_check_path)
        os.remove(stderr_check_path)
        out = f'{err} \n {out}'
        if "The analyzed formula is monitorable." not in out:
            return {'monitorable': False, 'message': out}
        else:
            return {'monitorable': True, 'message': out}
                    

    def make_dirs(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
    
    def db_is_empty(self) -> bool:
        return not os.path.exists(f'{self.sql_dir}drop.sql')
    
    def get_signature(self):
        sig = ''
        if not os.path.exists(self.sig):
            sig = 'no signature set'
        else:
            with open(self.sig, 'r') as sig_file:
                sig = sig_file.read()
                sig_file.close()
        return sig

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
    
    def restore_state(self):
        log = {}
        if os.path.exists(f'{self.sig_dir}/sig'):
            self.sig = f'{self.sig_dir}/sig'
            log |= {'restored sig': self.get_signature()}
        if os.path.exists(f'{self.pol_dir}/policy'):
            self.policy = f'{self.pol_dir}/policy'
            log |= {'restored policy': self.get_policy()}
        if os.path.exists(f'{self.monitor_logs}/monpoly_stdout.log'):
            self.launch()
            log |= {'tried restarting monpoly': self.get_monpoly_pid()}
        # TODO run events from database through monpoly
                
        return {'restore_state()': 'done'} | log

    def set_policy(self, policy):
        policy_location = f'{self.pol_dir}policy'
        # as long as monpoly isn't running yet, the policy can still be changed
        # TODO allow for policy change later on
        if os.path.exists(policy_location) and self.monpoly:
            return {'error': f'policy has already been set',
                    'policy_location': policy_location,
                    'ls pol_dir': os.listdir(self.pol_dir)}
        policy_location_abs = os.path.abspath(policy_location)
        os.rename(policy, policy_location_abs)
        self.policy = policy_location_abs
        return {'set policy': policy}

    def set_signature(self, sig):
        sig_location = f'{self.sig_dir}sig'
        # as long as monpoly isn't running yet, the policy can still be changed
        if os.path.exists(sig_location) and self.monpoly:
            return {'error': f'signature has already been set',
                    'sig_location': sig_location,
                    'ls sig_dir': os.listdir(self.sig_dir)}
        sig_location_abs = os.path.abspath(sig_location)
        os.rename(sig, sig_location_abs)
        self.sig = sig_location_abs
        init_log = self.init_database(self.sig)
        drop_log = self.get_destruct_query(self.sig)
        return init_log | drop_log

    def get_destruct_query(self, sig, verbose: bool = True):
        cmd_drop = f'monpoly -sql_drop {sig}'
        query_drop = run_command.runcmd(cmd_drop, verbose = False)

        if verbose: print(f'Generated drop query: {query_drop}')

        self.sql_drop_tables = query_drop
        location = f'{self.sql_dir}drop.sql'

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
        cmd_create = f'monpoly -sql {sig}'
        query_create = run_command.runcmd(cmd_create, verbose = False)
        self.db.run_query(query_create)
        return {'created tables': query_create}

    def spawn_monpoly(self, sig, pol):
        cmd = ['monpoly',
               '-unix',
               '-ignore_parse_errors',
               '-verbose', 
            #    '-debug', 'eval',
            #    '-log', './examples/logs/ex.log',
               '-sig', sig, 
               '-formula', pol
               ]
        # cmd_name = f'monpoly'
        # cmd_list = ['monpoly', '-unix', '-ignore_parse_errors', '-verbose', f'-sig {sig}', f'-formula {pol}']
        stdout = open(f'{self.monitor_logs}monpoly_stdout.log','w')
        stderr = open(f'{self.monitor_logs}monpoly_stderr.log','w')
        p = subprocess.Popen(cmd,
                            stdin=subprocess.PIPE,
                            stdout=stdout,
                            stderr=stderr,
                            text=True)
        return p

    def launch(self):
        if not self.sig:
            return 'no signature provided'
        elif not self.policy:
            return 'no policy provided'
        else:
            check = self.check_monitorability(self.sig, self.policy)
            if not check['monitorable']:
                return check['message']
                
            # self.init_database(self.sig)
            spawn_response = self.spawn_monpoly(self.sig, self.policy)
            if isinstance(spawn_response, str):
                return spawn_response
            else:
                self.monpoly = spawn_response
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
        # TODO store state
        if self.monpoly:
            self.monpoly.kill()
            # self.monpoly.close()

        return {'stopped': 'stopped monpoly'}

    def get_monpoly_pid(self):
        if self.monpoly:
            return self.monpoly.pid
            # return f'{self.monpoly}'
        else:
            return f'monpoly not running'

    def get_stdout(self):
        if not os.path.exists(f'{self.monitor_logs}monpoly_stdout.log'):
            return {'error': 'stdout log does not exist'}
        with open(f'{self.monitor_logs}monpoly_stdout.log', 'r') as stdout:
            return stdout.read() or 'stdout is empty'

    def get_stderr(self):
        stderr_path = f'{self.monitor_logs}monpoly_stderr.log'
        if not os.path.exists(stderr_path):
            return {'error': 'stderr log does not exist'}
        with open(stderr_path, 'r') as stderr:
            return stderr.read() or 'stderr is empty'
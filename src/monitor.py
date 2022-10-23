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

    def check_policy_and_signature_validity(self):
        return True

    def make_dirs(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
    
    def db_is_empty(self) -> bool:
        return not os.path.exists(f'{self.sql_dir}drop.sql')
    
    def get_signature(self):
        sig = ''
        if self.sig == '':
            sig = 'no signature set'
        else:
            with open(self.sig, 'r') as sig_file:
                sig = sig_file.read()
                sig_file.close()
        return sig

    def get_policy(self):
        policy = ''
        if self.policy == '':
            policy = 'no policy set'
        else: 
            with open(self.policy, 'r') as pol_file:
                policy = pol_file.read()
                pol_file.close()
        return policy
    
    def restore_state(self):
        # TODO
        log = {}
        if os.path.exists(f'{self.sig_dir}/sig'):
            self.sig = f'{self.sig_dir}/sig'
            log |= {'restored sig': self.get_signature()}
        if os.path.exists(f'{self.pol_dir}/policy'):
            self.policy = f'{self.pol_dir}/policy'
            log |= {'restored policy': self.get_policy()}
                
        return {'restore_state()': 'done'} | log

    def set_policy(self, policy):
        policy_location = f'{self.pol_dir}policy'
        if os.path.exists(policy_location):
            return {'error': f'policy has already been set',
                    'policy_location': policy_location,
                    'ls pol_dir': os.listdir(self.pol_dir)}
        policy_location_abs = os.path.abspath(policy_location)
        os.rename(policy, policy_location_abs)
        self.policy = policy_location_abs
        return {'set policy': policy}

    def set_signature(self, sig):
        sig_location = f'{self.sig_dir}sig'
        if os.path.exists(sig_location):
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
        cmd = f'monpoly -sig {sig} -formula {pol}'
        # TODO: run monpoly as a subprocess
        # return an object to which events can be passed to via stdin
        #TODO is `with open() as ...` the right way to do this?
        with open(f'{self.monitor_logs}monpoly_stdout.log', 'w') as stdout:
            with open(f'{self.monitor_logs}monpoly_stderr.log', 'w') as stderr:
                    p = subprocess.Popen([cmd],
                                        stdout=stdout,
                                        stderr=stderr,
                                        text=True,
                                        shell=True)
        return p

    def launch(self):
        # if not self.db_is_empty():
        #     self.restore_state()
        if not self.sig:
            return 'no signature provided'
        elif not self.policy:
            return 'no policy provided'
        else:
            if not self.check_policy_and_signature_validity():
                # TODO check that the policy only contains valid predicates
                return 'check if policy and signature match'
                
            # self.init_database(self.sig)
            self.monpoly = self.spawn_monpoly(self.sig, self.policy)

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
        return {'deleted everything': 'done'} | drop_log
    
    def stop(self):
        # TODO store state
        # TODO if monpoly is running, stop it. 
        # Python has some trouble with types as 
        # self.monpoly could have None type
        return {'stopped': 'stopped monpoly'}

    def get_stdout(self):
        with open(f'{self.monitor_logs}monpoly_stdout.log', 'r') as stdout:
            log = stdout.read()
            if log:
                return log
            else:
                return 'stdout is empty'

    def get_stderr(self):
        with open(f'{self.monitor_logs}monpoly_stdout.log', 'r') as stdout:
            log = stdout.read()
            if log:
                return log
            else:
                return 'stderr is empty'
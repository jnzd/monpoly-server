import json
import os
import subprocess
from datetime import datetime
from dateutil import parser
from dateutil.parser import ParserError
import psycopg2
from questdb.ingress import Buffer, Sender
from db_helper import DbHelper

# if this path is absolute all subsequent paths are relative to this path
# will be absolute paths
abspath = os.path.abspath(os.path.join(__file__, ".."))
dname = os.path.dirname(abspath)
os.chdir(dname)
CONFIG_DIR = os.path.abspath("./monitor-data/")
LOG_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

MONPOLY = 'monpoly' # './monpoly'
LOGGING = False # True
# LOGGING = True # True
TIMEPOINTS_TABLE = "time_points_unique_not_reserved_name"


class Monitor:
    """Wrapper class for MonPoly """
    def __init__(self):
        # should the policy be negated?
        self.policy_negate = False
        # database helper object
        self.db = DbHelper()
        # directory paths
        self.signature_dir = os.path.join(CONFIG_DIR, "signature")
        self.policy_dir = os.path.join(CONFIG_DIR, "policies")
        self.sql_dir = os.path.join(CONFIG_DIR, "sql")
        self.events_dir = os.path.join(CONFIG_DIR, "events")
        self.monpoly_stdout_dir = os.path.join(CONFIG_DIR, "monpoly-stdout")
        self.backend_data_dir = os.path.join(CONFIG_DIR, "backend-data")
        # create directories if they don't exist
        self.make_dirs(self.signature_dir)
        self.make_dirs(self.policy_dir)
        self.make_dirs(self.sql_dir)
        self.make_dirs(self.monpoly_stdout_dir)
        self.make_dirs(self.events_dir)
        self.make_dirs(self.backend_data_dir)
        self.conf_path = os.path.join(self.backend_data_dir, "conf.json")
        self.log_path = os.path.join(self.backend_data_dir, "backend.log")
        self.monitor_state_path = os.path.join(
            self.backend_data_dir, "monpoly_state.bin"
        )
        # paths to individual files
        self.signature_path = os.path.join(self.signature_dir, "signature.sig")
        self.sig_json_path = os.path.join(self.signature_dir, "sig.json")
        self.policy_path = os.path.join(self.policy_dir, "policy.mfotl")
        self.sql_drop_path = os.path.join(self.sql_dir, "drop.sql")
        self.monpoly_stdout_path = os.path.join(
            self.monpoly_stdout_dir, "monpoly_stdout.log"
        )
        self.monitorability_log_path = os.path.join(
            self.monpoly_stdout_dir, "monitorability.log"
        )

        self.most_recent_timestamp = None
        self.most_recent_timepoint = -1
        # second column isn't necessary for the functionality of the backend,
        # but questdb doesn't currently (2022-11-17) support tables with only
        # timestamp column:
        # https://github.com/questdb/questdb/issues/2691
        # self.ts_query_create = "CREATE TABLE ts(time_point INT,time_stamp TIMESTAMP) timestamp(time_stamp) PARTITION BY DAY;"
        self.ts_query_create = f"CREATE TABLE {TIMEPOINTS_TABLE}(time_point INT,time_stamp TIMESTAMP) timestamp(time_stamp);"
        self.ts_query_drop = f"DROP TABLE IF EXISTS {TIMEPOINTS_TABLE};"
        self.monpoly = None
        self.restore_state()
        self.write_config()

    def write_server_log(self, msg: str):
        """writes the given message to the server log along with a timestamp"""
        if LOGGING:
            time_stamp = datetime.now().strftime(LOG_TIMESTAMP_FORMAT)
            with open(self.log_path, "a", encoding="utf-8") as log:
                log.write(f"[{time_stamp}] {msg}\n")

    def check_monitorability(self, sig, pol, neg):
        """checks if the given policy is monitorable

        Args:
            sig (_type_): path to signature file
            pol (_type_): path to policy file

        Returns:
            dict: dictionary with keys "monitorable" and "message"
                "monitorable" is a boolean indicating if the policy is monitorable
                "message" is a string containing the output of monpoly checking
                    the monitorability of the policy
        """
        self.write_server_log(f"checking monitorability of {sig} and {pol} {'negated' if neg else ''}")
        cmd = [MONPOLY, "-check", "-sig", sig, "-formula", pol]
        if neg:
            cmd.append("-negate")
        # TODO: potentially set check to tur and report error to user
        process = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False
        )
        response = process.stdout
        with open(self.monitorability_log_path, "w", encoding="utf-8") as log:
            log.write(response)

        if "The analyzed formula is monitorable." not in response:
            return {"monitorable": False, "message": response}
        else:
            return {"monitorable": True, "message": response}

    def get_monitorability_log(self):
        """logged info on the monitorability of the current policy

        Returns:
            _type_: a string containing the monitorability log
        """
        if os.path.exists(self.monitorability_log_path):
            with open(self.monitorability_log_path, "r", encoding="utf-8") as log:
                return log.read()
        else:
            return "monitorability not yet checked"

    def signature_set(self):
        """returns true if the signature is set"""
        return self.signature_path and os.path.exists(self.signature_path)

    def policy_set(self):
        """returns true if the policy is set"""
        return self.policy_path and os.path.exists(self.policy_path)

    def make_dirs(self, path):
        """recursively creates all directories in the given path if they don't exist

        Args:
            path (_type_): a path to a directory
        """
        if not os.path.exists(path):
            os.makedirs(path)

    def db_is_empty(self) -> bool:
        """returns true if the database is empty"""
        return not os.path.exists(self.sql_drop_path)

    def get_signature(self):
        """get the signature

        Returns:
            _type_: either the signature or an error message or a string 
                "no signature set"
        """
        if os.path.exists(self.signature_path):
            with open(self.signature_path, "r", encoding="utf-8") as sig_file:
                return sig_file.read()
        else:
            return "no signature set"

    def get_json_signature(self):
        """get the current signature as a json object

        Returns:
            _type_: a dictionary either containing a key 'json' with the json
                signature or a key 'error' with an error message
        """
        if os.path.exists(self.sig_json_path):
            with open(self.sig_json_path, "r", encoding="utf-8") as sig_json:
                return {"json": json.load(sig_json)}
        else:
            return {"error": "json signature not set yet"}

    def get_policy(self):
        """get the policy being monitored

        Returns:
            _type_: the current policy being monitored
        """
        policy = ""
        if not os.path.exists(self.policy_path):
            policy = "no policy set"
        else:
            with open(self.policy_path, "r", encoding="utf-8") as pol_file:
                policy = pol_file.read()
                pol_file.close()
        return f'{policy} {"(negated)" if self.policy_negate else ""}'

    def get_schema(self):
        """get the database schema

        Returns:
            _type_: all tables in QuestDB
        """
        db_response = self.db.run_query("SHOW TABLES;", select=True)

        if "error" in db_response.keys():
            return db_response["error"]
        else:
            return db_response["response"]

    def get_config(self) -> dict:
        """get the monitor configuration

        Returns:
            dict: dictionary of the current monitor config
        """
        config = {
            "policy_negate": self.policy_negate,
            "db": self.db.get_config(),
            "most_recent_timestamp": datetime.strftime(
                self.most_recent_timestamp, LOG_TIMESTAMP_FORMAT
            )
            if self.most_recent_timestamp
            else None,
            "most_recent_timepoint": self.most_recent_timepoint,
        }
        return config

    def restore_db(self, conf):
        """restores the database connection from the config file

        Args:
            conf (_type_): dictionary with the database configuration
        """
        if "database" in conf.keys():
            self.db = DbHelper(conf["database"])
            self.write_server_log(
                f"Restored database connection: {self.db.get_config()}"
            )
        else:
            self.db = DbHelper()
            self.write_server_log(
                f"established database connection: {self.db.get_config()}"
            )

    def restore_state(self):
        """restores the state of the monitor from the config file"""
        # TODO: when get_config() gets changed, change this as well
        if os.path.exists(self.conf_path):
            with open(self.conf_path, "r", encoding="utf-8") as conf_json:
                conf = json.load(conf_json)
                self.policy_negate = conf["policy_negate"]
                ts = conf["most_recent_timestamp"]
                if ts is not None:
                    self.most_recent_timestamp = parser.parse(ts)
                self.most_recent_timepoint = conf["most_recent_timepoint"]
                self.restore_db(conf)
                self.write_server_log(f"[restore_state()] restored state with: {conf}")
        else:
            self.write_server_log(
                f"[restore_state()] config file doesn't exist: {self.conf_path}"
            )

    def write_config(self):
        """writes the current monitor config to disk
        """
        conf = self.get_config()
        with open(self.conf_path, "w", encoding="utf-8") as conf_json:
            conf_string = json.dumps(conf)
            conf_json.write(conf_string)
            self.write_server_log(f"wrote config: {conf_string}")

    def set_policy(self, policy, negate: bool = False):
        """sets the policy to the given policy

        Args:
            policy (_type_): path to a policy file
            negate (bool, optional): should the policy be negated?. Defaults to False.

        Returns:
            _type_: JSON style status message
        """
        # as long as monpoly isn't running yet, the policy can still be changed
        if os.path.exists(self.policy_path) and self.monpoly:
            return { "error": "monpoly is already running and policy has been set. Use change_policy() to change the policy." }
        os.rename(policy, self.policy_path)
        self.policy_negate = negate
        self.write_server_log(f"set policy: {self.get_policy()}")
        self.write_config()
        return {"message": f"policy set to {self.get_policy()}"}

    def get_relative_intervals(self, policy_path):
        """uses monpoly to get the relative intervals for the predicates in the policy

        Args:
            policy_path (_type_): path to a policy file

        Returns:
            _type_: the relative intervals per predicate and constant attributes in JSON
        """
        cmd = [
            MONPOLY,
            "-relative_interval_per_predicate_json",
            "-sig",
            self.signature_path,
            "-formula",
            policy_path,
        ]
        # TODO potentially set check to True and report errors to user
        process = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False
        )
        response_1 = process.stdout
        self.write_server_log(f"get_relative_intervals({policy_path}):\n {response_1}")
        cmd = [
            MONPOLY,  
            "-get_relative_interval",
            "-sig",
            self.signature_path,
            "-formula",
            policy_path,
        ]
        # TODO potentially set check to True and report errors to user
        process = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False
        )
        response_2 = process.stdout
        self.write_server_log(
            f"get_relative_intervals({policy_path}) [entire formula]: {response_2}"
        )
        return (response_2, json.loads(response_1))

    def change_policy(
        self,
        new_policy_path: str,
        negate: bool = False,
        naive: bool = False
    ):
        """changes the policy being monitored by monpoly

        Args:
            new_policy_path (str): path to the new policy
            negate (bool, optional): whether or not the new policy should be negated.
                Defaults to False.
            naive (bool, optional): whether or not the naive approach of reloacing
                the complete trace should be used
                Defaults to False.

        Returns:
            dict: JSON style status message
        """
        self.write_server_log(f"[change_policy()] negate: {negate}")
        if not os.path.exists(self.policy_path):
            self.write_server_log(
                f"[change_policy()] no policy has previously been set: {self.policy_path}"
            )
            return {
                "message": "no policy has been set previously, use /set-policy to set it",
                "ls pol_dir": os.listdir(self.policy_dir),
            }

        check = self.check_monitorability(self.signature_path, new_policy_path, self.policy_negate)
        if not check["monitorable"]:
            self.write_server_log(
                "[change_policy()] cannot change policy, because policy is not monitorable"
            )
            return {"error": check["message"]}
        # the events often take a while to propagate to the database and therefore a check is necessary if the most recent event is already in the database
        if self.most_recent_timepoint > -1:
            most_recent_timepoint_db = self.get_most_recent_timepoint_from_db()
            if most_recent_timepoint_db < self.most_recent_timepoint:
                return {
                    "error": f"Retry again later. Most recent timepoint seen is not in database yet: {most_recent_timepoint_db} (database) < {self.most_recent_timepoint} (monitor)"
                }

        relative_intervals = self.get_relative_intervals(new_policy_path)

        old_policy = self.get_policy()
        os.rename(new_policy_path, self.policy_path)
        self.policy_negate = negate
       # update negation in config
        self.write_config()
        self.write_server_log(
            f"[change_policy()] changed policy from {old_policy} to {self.get_policy()}"
        )
        if naive:
            timepoints = self.get_events()
        else:
            timepoints = self.get_events(relative_intervals=relative_intervals)
        # print(naive)
        # print(f"timepoints: {len(timepoints)}")
        timepoints_monpoly = os.path.join(self.events_dir, "events_policy_change.log")
        self.create_log_strings(timepoints, output_file=timepoints_monpoly)
        self.stop_monpoly(save_state=False)
        if timepoints == []:
            self.write_server_log(
                "[change_policy()] no timepoints found, starting monpoly without reading old timepoints"
            )
            self.monpoly = self.start_monpoly(self.signature_path, self.policy_path)
            self.write_server_log("[change_policy()] started monpoly")
        else:
            self.write_server_log(
                "[change_policy()] running monpoly and reading all past timepoints"
            )
            self.monpoly = self.start_monpoly(
                self.signature_path, self.policy_path, log=timepoints_monpoly
            )
            self.write_server_log("[change_policy()] started monpoly")
            if self.monpoly.stdout is None:
                return {"error": "monpoly stdout is None"}
            output = ""
            while "## Done with log file - waiting for stdin ##" not in output:
                self.write_server_log(
                    "[change_policy()] waiting for monpoly to finish"
                )
                output += self.monpoly.stdout.readline()
        self.clear_directory(self.events_dir)
        self.write_monpoly_log(
            f"--- policy changed from {old_policy} to {self.get_policy()} ---".replace(
                "\n", ""
            )
        )
        self.write_monpoly_log("\n")
        return {"success": f"changed policy from {old_policy} to {self.get_policy()}"}

    def set_signature(self, sig, db_exists=False):
        """sets the signature of the monitor, sets the database schema

        Args:
            sig (_type_): path to a signature file
            db_exists (bool, optional): whether or not the database already 
                exists. Defaults to False.

        Returns:
            _type_: JSON style status message
        """
        # as long as monpoly isn't running yet, the policy can still be changed
        if os.path.exists(self.signature_path):
            if self.monpoly:
                return {
                    "error": "signature has already been set",
                    "ls sig_dir": os.listdir(self.signature_dir),
                }
            else:
                self.delete_database()
        os.rename(sig, self.signature_path)
        if not db_exists:
            create_response = self.init_database(self.signature_path)
            if 'error' in create_response.keys():
                return create_response
        self.set_destruct_query(self.signature_path)
        self.create_json_signature(self.signature_path)
        self.write_server_log(f"set signature: {self.get_signature()}")
        self.write_config()
        return {"message": f"signature set to {self.get_signature()}"}

    def create_json_signature(self, sig):
        """asks monpoly to generate a json representation of the given signature

        Args:
            sig (_type_): path to a signature file

        Returns:
            _type_: json formatted signature
        """
        cmd = [MONPOLY, "-sig_to_json", sig]
        with open(self.sig_json_path, "w", encoding="utf-8") as json_sig:
            # TODO possibly set check to True and report errors to the user
            process = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if process.stderr:
                return {"error": f"create_json_signature: {process.stderr}"}
            json_sig.write(process.stdout)

        return self.get_json_signature()

    def set_destruct_query(self, sig):
        """asks monpoly to generate a drop query for the given signature

        Args:
            sig (_type_): path to a signature file

        Returns:
            _type_: JSON style status message
        """
        cmd = [MONPOLY, "-sql_drop", sig]
        # TODO possibly set check to True and report errors to the user
        process = subprocess.run(cmd, capture_output=True, text=True, check=False)
        query_drop = process.stdout
        query_drop += self.ts_query_drop
        self.write_server_log(
            f"[get_destruct_query()] Generated drop query: {query_drop}"
        )
        with open(self.sql_drop_path, "w", encoding="utf-8") as drop_file:
            drop_file.write(query_drop)
        return {"drop query": query_drop, "drop file": self.sql_drop_path}

    def init_database(self, sig):
        """
        Creates a database from the given signature file
        """
        cmd = [MONPOLY, "-sql", sig]
        # TODO possibly set check to True and report errors to the user
        process = subprocess.run(cmd, capture_output=True, text=True, check=False)
        query_create = process.stdout + self.ts_query_create
        create_response = self.db.run_query(query_create)
        self.write_server_log(f'ran queries: {query_create}\n\t with response: {create_response}')
        if 'error' in create_response.keys():
            return create_response
        # self.write_server_log(f'ran queries: {query_create} & {self.ts_query_create}')
        return {"success": create_response['response']}

    def start_monpoly(self, sig, pol, restart: str = "", log: str = ""):
        """starts monpoly with the given signature and policy

        Args:
            sig (_type_): path to a signature file
            pol (_type_): path to a policy file
            restart (str, optional): parameter whether this is a fresh start
                or a restart. Defaults to "".
            log (str, optional): path to a log file to be loaded,
                is used for policy change. Defaults to "".

        Returns:
            _type_: _description_
        """
        cmd = [
            MONPOLY,
            "-unix",
            "-ack_sep",
            "-ignore_parse_errors",
            "-tolerate_faulty_predicates",
            "-sig",
            sig,
            "-formula",
            pol,
        ]
        if restart:
            cmd.append("-load")
            cmd.append(restart)

        if self.policy_negate:
            cmd.append("-negate")

        if log != "":
            cmd.append("-log")
            cmd.append(log)
            cmd.append("-switch_to_stdin_after_log")
            cmd.append("-suppress_stdout")
            cmd.append("-nonewlastts")

        self.write_server_log(f"[spawn_monpoly()] cmd={cmd}")
        p = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        if not p.stdout:
            self.write_server_log(f"[spawn_monpoly()] monpoly_process.stdout is None")
        return p

    def launch(self, restart=False, db_exists=False):
        """
        starts or restarts monpoly and returns a string message
        """
        if self.monpoly and self.monpoly.poll() is None:
            self.write_server_log(
                f"[launch()] monpoly already running, self.monpoly.poll(): {self.monpoly.poll()}"
            )
            return "monpoly not started, because it is already running"
        self.write_server_log("[launch()] launching monpoly")
        if not self.signature_set():
            self.write_server_log(
                "[launch()] cannot launch monpoly, because signature is not set"
            )
            return "no signature provided"
        elif not self.policy_set():
            self.write_server_log(
                "[launch()] cannot launch monpoly, because policy is not set"
            )
            return "no policy provided"

        if not restart:
            check = self.check_monitorability(self.signature_path, self.policy_path, self.policy_negate)
            if not check["monitorable"]:
                self.write_server_log(
                    "[launch()] cannot launch monpoly, because policy is not monitorable"
                )
                return check["message"]

        if os.path.exists(self.monitor_state_path):
            self.write_server_log(
                f"[launch()] attempting to restart monpoly and load state from: {self.monitor_state_path}"
            )
            self.monpoly = self.start_monpoly(
                self.signature_path, self.policy_path, restart=self.monitor_state_path
            )
            return "restarted monpoly"

        if db_exists:
            return self.change_policy(self.policy_path, self.policy_negate)

        if not restart:
            self.monpoly = self.start_monpoly(self.signature_path, self.policy_path)
            self.write_server_log("launched monpoly")
            return {"pid": self.get_monpoly_pid(), "args": self.monpoly.args}
        else:
            return "cannot restart monpoly, because it was not previously started"

    def delete_database(self):
        """
        Deletes the database associated with the given signature file
        """
        query = ""
        if not self.db_is_empty():
            with open(self.sql_drop_path, "r", encoding="utf-8") as drop_file:
                query = drop_file.read()
                drop_file.close()
        elif self.db_is_empty():
            self.write_server_log(
                f"delete_database(): database is already empty (os.listdir({self.sql_dir}): {os.listdir(self.sql_dir)})"
            )
            return {"error": "Database is already empty"}

        self.write_server_log(
            f"delete_database(): deleting tables associated with {self.get_signature()}"
        )

        # TODO prompt user before running this query and deleting all tables
        query_response = self.db.run_query(query)
        os.remove(self.sql_drop_path)
        if "error" in query_response.keys():
            return query_response
        return {"query": query}

    def clear_directory(self, path):
        """empties the given directory

        Args:
            path (_type_): path to the directory to be emptied
        """
        self.write_server_log(f"clearing directory: {path}")
        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))

    def delete_config(self):
        """deletes the config file

        Returns:
            _type_: json style status message
        """
        if os.path.exists(self.conf_path):
            os.remove(self.conf_path)
        return {"config": f"deleted {self.conf_path}"}

    def delete_everything(self):
        """stops the monitor, clears the database, clears the config,
        empties config directories

        Returns:
            _type_: JSON style status message
        """
        stop_log = self.stop_monpoly(save_state=False)
        drop_log = self.delete_database()
        conf_log = self.delete_config()
        # self.clear_directory(CONFIG_DIR)
        self.clear_directory(self.signature_dir)
        self.clear_directory(self.policy_dir)
        self.clear_directory(self.events_dir)
        self.clear_directory(self.monpoly_stdout_dir)
        self.clear_directory(self.sql_dir)
        self.most_recent_timestamp = None
        self.most_recent_timepoint = -1
        self.write_config()
        if os.path.exists(self.monitor_state_path):
            os.remove(self.monitor_state_path)
        if os.path.exists(self.log_path):
            os.remove(self.log_path)
        return {"deleted everything": "done"} | drop_log | stop_log | conf_log

    def stop_monpoly(self, save_state: bool = True):
        """this stops monpoly and saves the state if save_state is True

        Args:
            save_state (bool, optional): parameter whether or not to save the 
                state of monpoly. Defaults to True.

        Returns:
            dict: JSON style status message
        """
        self.write_server_log("[stop()] stopping monpoly")
        log = dict()
        if not self.monpoly or self.monpoly.poll():
            self.write_server_log(
                f"[stop()] monpoly is not running, self.monpoly: {self.monpoly}"
            )
            return {"error": "monpoly not running or already stopped"}

        if self.monpoly and self.monpoly.poll() is None:
            if save_state and self.monpoly.stdin:
                self.write_server_log(
                    f"[stop()] sending > save_and_exit {self.monitor_state_path} <; to monpoly"
                )
                self.monpoly.stdin.write(
                    f"> save_and_exit {self.monitor_state_path} < ;"
                )
                self.monpoly.stdin.flush()
                self.write_server_log("[stop()] waiting for response from monpoly")
                return_code = self.monpoly.wait()
                self.write_server_log(
                    f"[stop()] monpoly exited with return code: {return_code}, self.monpoly.poll(): {self.monpoly.poll()}, saved state at {self.monitor_state_path}"
                )
                log |= {"stopped monpoly and stored sate, return code": return_code}
            elif not save_state:
                self.write_server_log("[stop()] stopping monpoly without saving state")
                self.monpoly.kill()
            else:
                self.write_server_log(
                    "[stop()] can't access stind of monpoly, stopping without saving state"
                )
                self.monpoly.kill()

        return {"stopped": "stopped monpoly"} | log

    def get_monpoly_pid(self):
        """returns the process id of monpoly

        Returns:
            _type_: the pid of monpoly or an error message if monpoly is not running
        """
        if self.monpoly:
            return self.monpoly.pid
        else:
            return "monpoly not running"

    def get_monpoly_exit_code(self):
        """returns the exit code of monpoly

        Returns:
            _type_: the exit code of monpoly or an error message if monpoly is still 
                running or has not been started yet
        """
        if self.monpoly:
            exit_code = self.monpoly.poll()
            if exit_code is not None:
                return exit_code
            else:
                return "monpoly still running"
        else:
            return "monpoly not running (yet)"

    def write_monpoly_log(self, log: str):
        """writes the stdout of monpoly to a file

        Args:
            log (str): the output to log
        """
        with open(self.monpoly_stdout_path, "a", encoding="utf-8") as monpoly_log:
            monpoly_log.write(log)

    def get_stdout(self) -> str:
        """reads the stdout of monpoly

        Returns:
            str: the stdout of monpoly or an error message if the 
                stdout does not exist
        """
        if not os.path.exists(self.monpoly_stdout_path):
            return "error stdout log does not exist"
        with open(self.monpoly_stdout_path, "r") as stdout:
            return stdout.read() or "stdout is empty"

    def get_most_recent_timestamp_from_db(self):
        """queries the most recent time stamp in the database

        Returns:
            _type_: the most recent time stamp seen by the database
        """
        try:
            query = f"SELECT MAX(time_stamp) FROM {TIMEPOINTS_TABLE};"
            t = self.db.run_query(query, select=True)
            if 'error' in t.keys():
                return None
            t = t['response']
            if t:
                return t[0][0]
            else:
                return None
        except psycopg2.DatabaseError:
            return None

    def get_most_recent_timepoint_from_db(self) -> int:
        """queries the most recent time point (index) in the database

        Returns:
            _type_: the most recent time stamp seen by the database
        """
        try:
            query = f"SELECT MAX(time_point) FROM {TIMEPOINTS_TABLE};"
            t = self.db.run_query(query, select=True)
            if 'error' in t.keys():
                return -1
            # database query result comes as a list of list
            # one list per table
            tp = t['response'][0][0]
            return int(tp) if tp is not None else -1
        except psycopg2.DatabaseError:
            return -1

    def store_timepoints_in_db(self, timepoints: list):
        """logs the given events in the database"""
        buf = Buffer()
        for timepoint in timepoints:
            if "skip" in timepoint.keys():
                continue
            self.most_recent_timestamp = datetime.fromtimestamp(timepoint["timestamp-int"])
            self.most_recent_timepoint = self.most_recent_timepoint + 1
            buf.row(TIMEPOINTS_TABLE, symbols=None, columns={"time_point": self.most_recent_timepoint}, at=self.most_recent_timestamp)
            for p in timepoint["predicates"]:
                if "name" not in p.keys():
                    return {"log_events error": 'predicate must have a "name"'}
                elif "occurrences" not in p.keys():
                    # predicate can be named without an occurrence
                    continue
                name = p["name"]
                for occ in p["occurrences"]:
                    columns = {"time_point": self.most_recent_timepoint} | {
                        f"x{i+1}": o for i, o in enumerate(occ)
                    }
                    buf.row(name, symbols=None, columns=columns, at=self.most_recent_timestamp)
        # update config after going over all timestamps
        self.write_config()
        with Sender(self.db.host, self.db.port_influxdb) as sender:
            self.write_server_log(f"sending buffer {buf} to database")
            sender.flush(buf)

        return {"events": timepoints}

    def send_timepoint_to_monpoly(self, event_str: str):
        """sends the given events to MonPoly

        Args:
            event_str (str): string of events formatted as MonPoly input

        Returns:
            _type_: JSON style response message
        """
        if self.monpoly:
            if self.monpoly.stdin and self.monpoly.stdout:
                self.write_server_log(
                    f"[send_events_to_monpoly({event_str})] sending events to monpoly: {event_str}"
                )
                self.monpoly.stdin.write(event_str)
                self.monpoly.stdin.flush()
                result = ""
                reached_separator = False
                while not reached_separator:
                    line = self.monpoly.stdout.readline()
                    self.write_server_log(
                        f"[send_events_to_monpoly({event_str})] read line from monpoly: {line}"
                    )
                    reached_separator = "## reached separator ##" in line
                    if not reached_separator:
                        result += line

                self.write_monpoly_log(result)
                self.write_server_log(
                    f"[send_events_to_monpoly({event_str})] monpoly done - stdout: {result}"
                )
                return {"success": f'sent "{event_str}" to monpoly', "output": result}

            self.write_server_log(
                f"could not access stdin or stdout of monpoly (stdout:{self.monpoly.stdout}, stdin:{self.monpoly.stdin})"
            )
            return {"error": "Error while logging events monpoly stdin is None"}

        self.write_server_log("error: monpoly is not running")
        return {"error": "Monpoly is not running"}

    def create_log_strings(self, timepoints: list, output_file=None):
        """
        this function takes a list of event dictionaries
        it adds log strings (to be sent to monpoly) to for
        each timestamp and returns the extended list of
        dictionaries
        """
        self.write_server_log(f"create_log_strings({timepoints})")
        for timepoint in timepoints:
            timestamp = timepoint["timestamp-int"]
            monpoly_string = f"@{timestamp} "

            for predicate in timepoint["predicates"]:
                if "name" not in predicate.keys():
                    timepoint["skip"] = f"predicate {predicate} has no name"
                    self.write_server_log(
                        f"create_log_strings(): predicate ({predicate}) with no name at timestamp: {timestamp}"
                    )
                    break
                else:
                    name = predicate["name"]
                    for occurrence in predicate["occurrences"]:
                        predicate_str = (
                            f"{name} {self.tuple_str_from_list(occurrence)} "
                        )
                        monpoly_string += predicate_str
            monpoly_string += ";\n"
            timepoint["monpoly-string"] = monpoly_string
            if output_file is not None:
                with open(output_file, "a", encoding="utf-8") as f:
                    f.write(monpoly_string)

            self.write_server_log(
                f"create_log_strings(): created monpoly string: {monpoly_string}"
            )
        return timepoints

    def tuple_str_from_list(self, l: list) -> str:
        """converts a list into a string that can be used as a tuple in monpoly.
        Importantly it doesn't have a trailing comma before the closing bracket

        Args:
            l (list): lsit of attributes

        Returns:
            str: tuple string for monpoly (e.g. (1, 2, 3))
        """
        l_str = [str(x) for x in l]
        return "(" + ", ".join(l_str) + ")"

    def get_timestamp(self, event: dict, timestamp_now: datetime) -> int:
        """
        This method checks if the event has a timestamp
        it sets it to the current time if it doesn't
        It returns a timestamp in seconds since 1970-01-01 00:00:00
        (in monpoly/scr/formula_parser.mly:timeunits it can be seen that
        seconds are the smallest and default time unit in monpoly)
        If timestamp_now has no timezone info, it is assumed to be UTC
        """
        if "timestamp" in event.keys():
            try:
                ts = parser.parse(event["timestamp"])
            except ParserError:
                # TODO: is this desirable or should this timepoint be ignored and skipped?
                # could return -1 and later check if 'timestamp-int' is -1
                ts = timestamp_now
        else:
            ts = timestamp_now
        if ts.tzinfo is None:
            ts = ts.astimezone()
            ts = ts + ts.utcoffset()
        return int(ts.timestamp())

    def log_timepoints(self, timepoints_json: str) -> dict:
        """logs the events in the given json file
        first checking the JSON formatting, then sending it to MonPoly and if
        MonPoly accepts it, it is written to the database

        Args:
            timepoints_json (str): path to the json file containing the events

        Returns:
            dict: JSON style response dcitionary with either success message
                or error message
        """
        # get current time at this point, so all events with a missing timestamp are logged with the same timestamp
        self.write_server_log(
            f"[log_timepoints()] started logging events: {timepoints_json}"
        )
        timestamp_now = datetime.now()
        with open(timepoints_json, encoding="utf-8") as f:
            try:
                timepoints = json.load(f)
                timepoints = [
                    {"timestamp-int": self.get_timestamp(e, timestamp_now)} | e
                    for e in timepoints
                ]
                # TODO don't sort - leave order of time points up to user and skip if out of order
                # list.sort(timepoints, key=lambda e: e["timestamp-int"])
                timepoints = self.create_log_strings(timepoints)

                skip_log = {}
                for timepoint in timepoints:
                    if "skip" in timepoint.keys():
                        self.write_server_log(
                            f'[log_timepoints()] skipping event: {timepoint}, because: {timepoint["skip"]}'
                        )
                        skip_log |= {timepoint["timestamp-int"]: timepoint["skip"]}
                        continue
                    monpoly_output = self.send_timepoint_to_monpoly(
                        timepoint["monpoly-string"]
                    )
                    if "error" in monpoly_output.keys():
                        return {
                            "error": f'error while logging timepoints: {monpoly_output["error"]}'
                        }
                    output = monpoly_output["output"]
                    if (
                        "WARNING: Skipping out of order timestamp" in output
                        or "ERROR" in output
                    ):
                        timepoint["skip"] = output
                        skip_log |= {timepoint["timestamp-int"]: timepoint["skip"]}
                db_response = self.store_timepoints_in_db(timepoints)
                self.write_server_log(f"stored events in db: {db_response}")

                return {"skipped-timepoints": skip_log}

            except ValueError as error:
                self.write_server_log(f"error parsing json file: {error}")
                self.clear_directory(self.events_dir)
                return {"error": f"Error while parsing events JSON {error}"}

    def db_response_to_timepoints(self, db_response: list) -> list:
        """converts the response from the database to a list of timepoints
        in the same format that the wrapper uses as input

        Args:
            db_response (list): a list responses to a number of SQL queries

        Returns:
            list: a list of evenets per time point in a JSON style list of dictionaries
        """
        self.write_server_log(
            "[db_response_to_timepoints()] converting db response to timepoints"
        )
        db_response_dict = {k: v for d in db_response for k, v in d.items()}
        if db_response_dict[TIMEPOINTS_TABLE] is not None:
            timestamps = {x[1] for x in db_response_dict[TIMEPOINTS_TABLE] if x is not None}
        else:
            return []
        result = dict()
        for ts in timestamps:
            ts_int = int(ts.timestamp())
            ts_dict = {
                "timestamp-int": ts_int,
                "timestamp": ts.strftime(LOG_TIMESTAMP_FORMAT),
                "predicates": dict(),
            }
            result[ts_int] = ts_dict

        for predicate_name in db_response_dict.keys():
            if predicate_name == TIMEPOINTS_TABLE:
                continue
            for occurrence in db_response_dict[predicate_name]:
                ts = int(occurrence[-1].timestamp())
                # TODO work with time_point column
                if predicate_name in result[ts]["predicates"].keys():
                    result[ts]["predicates"][predicate_name].append(occurrence[0:-2])
                    result[ts]["timepoint"] = occurrence[-2]
                else:
                    result[ts]["predicates"][predicate_name] = [occurrence[0:-2]]
                    result[ts]["timepoint"] = occurrence[-2]

        result = [v for _, v in result.items()]
        for t in result:
            t["predicates"] = [
                {"name": k, "occurrences": v} for k, v in t["predicates"].items()
            ]
        result.sort(key=lambda e: e["timestamp-int"])

        return result

    def parse_masked_interval(self, mask: list, interval: str) -> str:
        """combines a list of mask values and a string represntation of an interval
        into a SQL query suffix

        Args:
            mask (list): list of constant predicate attributs. None if the 
                attribute at the given position is not constant
            interval (str): string representation of an interval (e.g. "[1,2)")

        Returns:
            str: a SQL query constraint (e.g. "(x1 = 1 AND x2 = 2 AND [1,2))")
        """
        named_mask = [
            (f"x{i+1} = {y}", interval)
            for (i, y), interval in zip(enumerate(mask), interval)
            if y is not None
        ]
        mask_query = " AND ".join([x[0] for x in named_mask])
        if mask_query == "":
            return f"({interval})"
        return f"({mask_query} AND {interval})"

    def parse_interval(self, i: str) -> str:
        """pareses a string representation of an interval into a SQL query suffix
        (e.g. "(1,2)" -> "time_stamp > '1970-01-01 00:00:01' AND time_stamp < '1970-01-01 00:00:02'")

        Args:
            i (str): the string representation of the interval

        Returns:
            str: the SQL query suffix
        """
        if self.most_recent_timestamp is not None:
            t = datetime.timestamp(self.most_recent_timestamp)
        else:
            t = 0

        is_lower_open = i[0] == "("
        is_upper_open = i[-1] == ")"
        bounds = i[1:-1].split(",")
        # upper = datetime.utcfromtimestamp(int(bounds[1]) + t)
        # upper = datetime.utcfromtimestamp(t)
        # The upper bound is always the current time, `t`, as there
        upper_int = min(int(bounds[1]) + t, t)
        upper = datetime.utcfromtimestamp(upper_int)
        lower = datetime.utcfromtimestamp(int(bounds[0]) + t)
        self.write_server_log(f"interval: {i} + {self.most_recent_timestamp} -> lower: {lower}, upper: {upper}")
        if upper == "*" and lower == "*":
            query = ""
        elif upper == "*":
            query = f'time_stamp {">" if is_lower_open else ">="} \'{lower}\''
        elif lower == "*":
            query = f'time_stamp {"<" if is_upper_open else "<="} \'{upper}\''
        else:
            query = f'time_stamp {">" if is_lower_open else ">="} \'{lower}\' AND time_stamp {"<" if is_upper_open else "<="} \'{upper}\''
        return query

    def relative_intervals_to_query_per_predicate(
        self, predicate_name: str, intervals: list
    ) -> str:
        """creates a query for a given predicate and its masked relative intervals

        Args:
            predicate_name (str): the name of the predicate
            intervals (list): list of dictionaries with keys "mask" and "interval"
                the mask part are the constant values and the interval is the
                relative interval corresponding to the predicate with the constant
                values given by the mask

        Returns:
            str: a single SQL query for the given predicate and its masked relative intervals
        """
        prefix = f"SELECT * FROM {predicate_name} WHERE "
        conditions = []
        for masked_interval in intervals:
            mask = masked_interval["mask"]
            interval = masked_interval["interval"]
            parsed_interval = self.parse_interval(interval)
            parsed_masked_interval = self.parse_masked_interval(mask, parsed_interval)
            conditions.append(parsed_masked_interval)

        suffix = " OR ".join(conditions) + ";"
        return prefix + suffix

    def relative_intervals_to_query(
        self, relative_intervals: tuple
    ) -> list[tuple[str, str]]:
        """for the given predicates and their relative intervals, this generates
        SQL queries corresponding to those relative intervals and the predicates

        Args:
            relative_intervals (tuple): tuple of the relative interval for the whole 
                formula and a list of dictionaries containing the relative intervals
                for each predicate in the formula

        Returns:
            list[tuple[str, str]]: list of tuples with one query for each predicate
                and the ts table (a kind of internal predicate)
        """
        queries = []
        rl, rls = relative_intervals
        for predicate in rls:
            name = predicate["predicate_name"]
            intervals = predicate["intervals"]
            query = self.relative_intervals_to_query_per_predicate(name, intervals)
            queries.append((name, query))
        parsed_interval = self.parse_interval(rl)
        query = f"SELECT * FROM {TIMEPOINTS_TABLE} WHERE {parsed_interval};"
        queries.append((TIMEPOINTS_TABLE, query))
        return queries

    def queries_from_dates(
        self, start_date=None, end_date=None
    ) -> list[tuple[str, str]]:
        """generates a list of tuples containing predicate name and a SQL query
        that returns all the occurrences of that predicate in the given time interval

        Args:
            start_date (_type_, optional): Defaults to None.
            end_date (_type_, optional): Defaults to None.

        Returns:
            list[tuple[str, str]]: list of tuples of predicate name and SQL query
        """
        queries = []
        names = []
        if os.path.exists(self.sig_json_path):
            with open(self.sig_json_path) as f:
                signature = json.load(f)
                for predicate in signature:
                    names.append(predicate["name"])

        if start_date is not None and end_date is not None:
            # BETWEEN is inclusive
            query_suffix = f"WHERE time_stamp BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date is not None:
            query_suffix = f"WHERE time_stamp >= '{start_date}'"
        elif end_date is not None:
            query_suffix = f"WHERE time_stamp <= '{end_date}'"
        else:
            query_suffix = ""

        names.append(TIMEPOINTS_TABLE)
        for predicate_name in names:
            query = f"SELECT * FROM {predicate_name} {query_suffix};"
            queries.append((predicate_name, query))
        return queries

    def get_events(
        self, relative_intervals=None, start_date=None, end_date=None
    ) -> list:
        """ retrieves all events in the database

        Args:
            relative_intervals (_type_, optional): 
                if this is provided for each predicate only the events 
                in its relative interval are retrieved. Defaults to None.
            start_date (_type_, optional): Defaults to None.
            end_date (_type_, optional): Defaults to None.

        Returns:
            list: all events in the database
        """
        queries = []
        if relative_intervals is not None:
            queries = self.relative_intervals_to_query(relative_intervals)
        else:
            queries = self.queries_from_dates(start_date, end_date)

        # TODO the queries could potentially be combined into a single string
        #      possibly not feasible, because cursor.fetchall() inside
        #      db.run_query() would return a list of tuples, where it isn't clear
        #      which tuple belongs to which predicate

        # query = "/n".join([q[1] for q in queries])
        # self.write_server_log(f"    running query: {query}")
        # response = self.db.run_query(query, select=True)
        # if 'error' in response.keys():
        #     return response['error']
        # results.append({predicate_name: response['response']})

        results = []
        self.write_server_log(f"    current timestamp: {self.most_recent_timestamp}")
        for predicate_name, query in queries:
            self.write_server_log(f"    running query: {query}")
            response = self.db.run_query(query, select=True)
            if 'error' in response.keys():
                return response['error']
            results.append({predicate_name: response['response']})

        monpoly_log = self.db_response_to_timepoints(results)
        return monpoly_log

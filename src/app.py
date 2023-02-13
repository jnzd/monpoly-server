import os
import atexit
from werkzeug.utils import secure_filename
from flask import Flask, request, flash
from dateutil import parser
from dateutil.parser import ParserError
from monitor import Monitor

app = Flask(__name__, static_folder="./static")

# TODO make this actually secure
app.secret_key = "super secret key"
app.config["SESSION_TYPE"] = "filesystem"

abspath = os.path.abspath(os.path.join(__file__, ".."))
dname = os.path.dirname(abspath)
os.chdir(dname)

mon = Monitor()


@app.before_first_request
def before_first_request():
    mon.restore_state()
    restart_response = mon.launch(restart=True)
    mon.write_server_log(f"app.py before_first_request() done: {restart_response}")


def exit_handler():
    mon.stop_monpoly()
    mon.write_server_log(f"app.py exit_handler() done")


atexit.register(exit_handler)


def string_to_html(text):
    return text.replace("\n", "<br>")


@app.route("/", methods=["GET"])
def index():
    content = f""" 
        <h1>Monpoly Backend</h1>
        <p>
        <b>You are monitoring the following policy:</b><br> {string_to_html(mon.get_policy())} <br>
        <b>With the signature:</b><br> {string_to_html(mon.get_signature())}  
        </p>
        <h2>Database schema</h2>
        <p>
        {mon.get_schema()}
        </p>
        <h2>Monitor process information</h2>
        <p> {mon.get_monpoly_pid()}: {mon.monpoly.args if mon.monpoly and mon.monpoly.args else ""} </p>
        <p> exit code: {mon.get_monpoly_exit_code()} </p>
        <h3>Monitorability</h3>
        <p> {string_to_html(mon.get_monitorability_log())} </p>
        <h2>Monitor log</h2>
        <p> {string_to_html(mon.get_stdout())} <p>
    """
    return content


@app.route("/get-policy", methods=["GET", "POST"])
def get_policy():
    return {"policy": "not implemented yet"}


@app.route("/set-policy", methods=["POST"])
def set_policy():
    """
    this sets the policy
    """
    if "policy" not in request.files:
        return {
            "message": "no file provided, for curl use `-F` and not `-d`",
            "policy (POST)": mon.get_policy(),
        }
    pol_file = request.files["policy"]
    if pol_file == "":
        return {"message": "filename can't be empty", "policy": mon.get_policy()}
    else:
        filename = secure_filename(pol_file.filename)  # type: ignore
        path = os.path.join(mon.policy_dir, filename)
        pol_file.save(path)
        negate = "negate" in request.form
        return mon.set_policy(path, negate)


@app.route("/change-policy", methods=["POST"])
def change_policy():
    if "policy" not in request.files:
        return {
            "message": "no file provided, for curl use `-F` and not `-d`",
            "policy (POST)": mon.get_policy(),
        }
    pol_file = request.files["policy"]
    if pol_file == "":
        return {"message": "filename can't be empty", "policy": mon.get_policy()}
    else:
        filename = secure_filename(pol_file.filename)  # type: ignore
        path = os.path.join(mon.policy_dir, filename)
        pol_file.save(path)
        negate = "negate" in request.form
        # TODO later check for parameter specifying policy change method
        return mon.change_policy(path, negate)


@app.route("/get-signature", methods=["GET"])
def get_signature():
    return {"signature": mon.get_signature()}


@app.route("/set-signature", methods=["POST"])
def set_signature():
    """
    this sets the signature if it has not been set yet
    """
    if "signature" not in request.files:
        flash("No signature part")
        return {
            "message": "no file provided, for curl use `-F` and not `-d`",
            "signature (POST)": mon.get_signature(),
        }

    sig_file = request.files["signature"]

    if sig_file == "":
        flash("No selected file")
        return {
            "message": "filename can't be empty",
            "signature (POST-empty-name)": mon.get_signature(),
        }
    else:
        filename = secure_filename(sig_file.filename)  # type: ignore
        path = os.path.join(mon.signature_dir, filename)
        sig_file.save(path)
        return mon.set_signature(path)


@app.route("/start-monitor", methods=["GET", "POST"])
def start_monitor():
    use_existing_db = False
    if "existing-db" in request.form:
        use_existing_db = True
    launch_msg = mon.launch(db_exists=use_existing_db)
    return {"launch message": launch_msg}


@app.route("/stop-monitor", methods=["GET", "POST"])
def stop_monitor():
    return mon.stop_monpoly()


@app.route("/reset-everything", methods=["GET"])
def reset_monitor():
    delete_message = mon.delete_everything()
    return delete_message


@app.route("/log-events", methods=["POST"])
def log():
    """
    takes events with or without timestamps in json format
    """
    if "events" not in request.files:
        flash("No events sent")
        return {"message": "no events provided, for curl use `-F` and not `-d`"}

    events_file = request.files["events"]
    if events_file == "":
        flash("No selected file")
        return {"message": "filename can't be empty"}
    else:
        filename = secure_filename(events_file.filename)  # type: ignore
        path = os.path.join(mon.events_dir, filename)
        events_file.save(path)
        result = mon.log_timepoints(path)
        return result


@app.route("/get-events", methods=["GET"])
def get_events():
    start_date = None
    if "start" in request.form:
        try:
            start_date = parser.parse(request.form["start"])
        except ParserError:
            return {"error": f'invalid end date: {request.form["start"]}'}

    end_date = None
    if "end" in request.form:
        try:
            end_date = parser.parse(request.form["end"])
        except ParserError:
            return {"error": f'invalid end date {request.form["end"]}'}

    return mon.get_events(start_date=start_date, end_date=end_date)


@app.route("/get-most-recent", methods=["GET"])
def get_most_recent():
    return {"response": mon.get_most_recent_timestamp_from_db()}

## Database configuration methods

@app.route("/db-set-user", methods=["POST"])
def db_set_user():
    if "user" not in request.form:
        return {"error": "no user provided"}
    user = request.form["user"]
    try:
        mon.db.set_user(user)
        mon.write_config()
        return {"response": f"set user to {user}"}
    except Exception as e:
        return {"error": str(e)}

@app.route("/db-set-password", methods=["POST"])
def db_set_password():
    if "password" not in request.form:
        return {"error": "no password provided"}
    password = request.form["password"]
    try:
        mon.db.set_password(password)
        mon.write_config()
        return {"response": f"set password to {password}"}
    except Exception as e:
        return {"error": str(e)}

@app.route("/db-set-host", methods=["POST"])
def db_set_host():
    if "host" not in request.form:
        return {"error": "no host provided"}
    host = request.form["host"]
    try:
        mon.db.set_host(host)
        mon.write_config()
        return {"response": f"set host to {host}"}
    except Exception as e:
        return {"error": str(e)}

@app.route("/db-set-pgsql-port", methods=["POST"])
def db_set_pgsql_port():
    if "port" not in request.form:
        return {"error": "no port provided"}
    port = request.form["port"]
    try:
        mon.db.set_pgsql_port(port)
        mon.write_config()
        return {"response": f"set port to {port}"}
    except Exception as e:
        return {"error": str(e)}

@app.route("/db-set-influxdb-port", methods=["POST"])
def db_set_influxdb_port():
    if "port" not in request.form:
        return {"error": "no port provided"}
    port = request.form["port"]
    try:
        mon.db.set_influxdb_port(port)
        mon.write_config()
        return {"response": f"set port to {port}"}
    except Exception as e:
        return {"error": str(e)}

@app.route("/db-set-database", methods=["POST"])
def db_set_database():
    if "database" not in request.form:
        return {"error": "no database provided"}
    database = request.form["database"]
    try:
        mon.db.set_database(database)
        mon.write_config()
        return {"response": f"set database to {database}"}
    except Exception as e:
        return {"error": str(e)}

@app.route("/db-get-user", methods=["GET"])
def db_get_user():
    return {"response": mon.db.get_user()}

@app.route("/db-get-password", methods=["GET"])
def db_get_password():
    return {"response": mon.db.get_password()}

@app.route("/db-get-host", methods=["GET"])
def db_get_host():
    return {"response": mon.db.get_host()}

@app.route("/db-get-pgsql-port", methods=["GET"])
def db_get_pgsql_port():
    return {"response": mon.db.get_pgsql_port()}

@app.route("/db-get-influxdb-port", methods=["GET"])
def db_get_influxdb_port():
    return {"response": mon.db.get_influxdb_port()}

@app.route("/db-get-database", methods=["GET"])
def db_get_database():
    return {"response": mon.db.get_database()}
    
if __name__ == '__main__':
  app.run()

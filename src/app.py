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
    # TODO on keyboard interrupt this doesn't work, because
    #      the sigint already got forwarded to the child process
    # possible solution would be to add a flag to monpoly
    # which makes it such that the state gets saved on keyboard interrupt
    # alternatively the state could be saved on every step, but this
    # seems unnecessarily expensive
    # ---- should be fixed by 'start_new_session=True' in subprocess.Popen
    mon.stop_monpoly()
    mon.write_server_log(f"app.py exit_handler() done")


atexit.register(exit_handler)

# TODO add method to send database credentials
# for now the defaults are used, when questdb is running locally


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
    # TODO check if signature and/or policy are already set
    # if not check if they are sent with this request
    # if neither return message to set first
    launch_msg = mon.launch()
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

from flask import Flask, request, flash, redirect
from monitor import Monitor
from werkzeug.utils import secure_filename
import os
from log_events import log_events

app = Flask(__name__)

@app.before_first_request
def before_first_request():
    log = mon.restore_state()
    print(log)

# TODO make this actually secure
app.secret_key = 'super secret key'
app.config['SESSION_TYPE'] = 'filesystem'

mon = Monitor()

# TODO add method to send database credentials
# for now the defaults are used, when questdb is running locally

@app.route('/', methods=['GET'])
def index():
    return '<h1>Monpoly Backend</h1>'

@app.route('/get-policy', methods=['GET', 'POST'])
def get_policy():
    return {'policy': 'not implemented yet'}

@app.route('/set-policy', methods=['POST'])
def set_policy():
    '''
    this sets the policy
    '''
    # TODO allow for changes to the policy
    if 'policy' not in request.files:
        flash('No policy part')
        return {'message': 'no file provided, for curl use `-F` and not `-d`', 
                'signature (POST)': mon.get_policy()}
    sig_file = request.files['signature']
    if sig_file == '':
        flash('No selected file')
        return {'message': 'filename can\'t be empty',
                'signature (POST-empty-name)': mon.get_signature()}
    else:
        filename = secure_filename(sig_file.filename)  # type: ignore
        path = os.path.join(mon.sig_dir, filename)
        sig_file.save(path)
        return mon.set_signature(path)

@app.route('/get-signature', methods=['GET'])
def get_signature():
    return {'signature': mon.get_signature()}

@app.route('/set-signature', methods=['POST'])
def set_signature():
    '''
    this sets the signature if it has not been set yet
    '''
    if 'signature' not in request.files:
        flash('No signature part')
        return {'message': 'no file provided, for curl use `-F` and not `-d`', 
                'signature (POST)': mon.get_signature()}
    
    sig_file = request.files['signature']

    if sig_file == '':
        flash('No selected file')
        return {'message': 'filename can\'t be empty',
                'signature (POST-empty-name)': mon.get_signature()}
    else:
        filename = secure_filename(sig_file.filename)  # type: ignore
        path = os.path.join(mon.sig_dir, filename)
        sig_file.save(path)
        return mon.set_signature(path)


@app.route('/start-monitor', methods=['GET', 'POST'])
def start_monitor():
    # TODO check if signature and/or policy are already set
    # if not check if they are sent with this request
    # if neither return message to set first
    mon.launch()
    return dict()

@app.route('/stop-monitor', methods=['GET', 'POST'])
def stop_monitor():
    mon.stop()
    return dict()

@app.route('/reset-everything', methods=['GET'])
def drop_tables():
    stop_message = mon.stop()
    return stop_message

@app.route('/log-events', methods=['POST'])
def log():
    '''
    takes events with or without timestamps in json format
    '''
    if 'events' not in request.files:
        flash('No events sent')
        return {'message': 'no events provided, for curl use `-F` and not `-d`'}
    events_file = request.files['events']
    if events_file == '':
        flash('No selected file')
        return {'message': 'filename can\'t be empty'}
    else:
        filename = secure_filename(events_file.filename)  # type: ignore
        
    return {'logging event':0}

    
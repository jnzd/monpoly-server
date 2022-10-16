from flask import Flask, request, flash, redirect
from monitor import monitor
from werkzeug.utils import secure_filename
import os

app = Flask(__name__)
app.secret_key = 'super secret key'
app.config['SESSION_TYPE'] = 'filesystem'

mon = monitor()

# TODO add method to send database credentials
# for now the defaults are used, when questdb is running locally

@app.route("/print_filename", methods=['POST'])
def print_filename():
    file_ = request.files['file']
    if file_:
        filename=secure_filename(file_.filename)     # type: ignore
        return filename
    return "No file"

@app.route('/signature', methods=['GET', 'POST'])
def signature():
    global mon
    if request.method == 'GET':
        return {'signature (GET)': mon.get_signature()}
    elif request.method == 'POST':
        # return {'signature (POST)': request.form['signature']}
        # return request.files, request.form, request.args
        if 'signature' not in request.files:
            flash('No signature part')
            return {'message': 'no file provided, for curl use `-F` and not `-d`', 
                    'signature (POST)': mon.get_signature()}
            # return redirect(request.url)
        
        sig_file = request.files['signature']

        if sig_file == '':
            flash('No selected file')
            return {'message': 'filename can\'t be empty',
                    'signature (POST-empty-name)': mon.get_signature()}
            # return redirect(request.url)

        if sig_file:
            filename = secure_filename(sig_file.filename)  # type: ignore
            path = os.path.join(mon.signature_directory, filename)
            sig_file.save(path)
            print(path)
            query = mon.db.create_database(path)
            return {'query': query,
                    'signature (POST-created-db)': mon.get_signature()}
            return redirect(request.url)
    else:
        return 'unsupported request type'
    
    return {'signature (default)': mon.get_signature()}

@app.route('/drop', methods=['GET'])
def drop_tables():
    global mon
    query = mon.db.delete_database()
    return {'query': query}

@app.get('/log-event')
def log_event():
    return {"logging event":0}
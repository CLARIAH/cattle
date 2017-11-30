#!/usr/bin/env python

# cattle: A Web service for COW

from flask import Flask, request, render_template, make_response, redirect, jsonify
from werkzeug.utils import secure_filename
import logging
import requests
import os
import subprocess
from subprocess import check_output
import json
from rdflib import ConjunctiveGraph

# The Flask app
app = Flask(__name__)

# Uploading
UPLOAD_FOLDER = '/tmp'
ALLOWED_EXTENSIONS = set(['csv', 'json'])
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Logging
LOG_FORMAT = '%(asctime)-15s [%(levelname)s] (%(module)s.%(funcName)s) %(message)s'
app.debug_log_format = LOG_FORMAT
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
cattlelog = logging.getLogger(__name__)

# Accepted content types
ACCEPTED_TYPES = ['application/ld+json',
                  'application/n-quads',
                  'text/turtle',
                  'application/ld+json; profile="http://www.w3.org/ns/activitystreams', 'turtle', 'json-ld', 'nquads']

# Util functions

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def upload_cleanup():
    files = os.listdir(app.config['UPLOAD_FOLDER'])
    for file in files:
        if file.endswith(".csv") or file.endswith(".json") or file.endswith(".nq") or '-metadata.' in file:
            os.remove(os.path.join(app.config['UPLOAD_FOLDER'], file))

# Routes

@app.route('/', methods=['GET', 'POST'])
def cattle():
    cattlelog.info("Received request to render index")
    resp = make_response(render_template('index.html', version=check_output(['cow_tool', '--version'], stderr=subprocess.STDOUT).strip()))
    resp.headers['X-Powered-By'] = 'https://github.com/albertmeronyo/cattle'

    return resp

@app.route('/version', methods=['GET', 'POST'])
def version():
    v = check_output(['cow_tool', '--version'], stderr=subprocess.STDOUT)
    cattlelog.debug("Version {}".format(v))

    return v

@app.route('/build', methods=['POST'])
def build():
    cattlelog.info("Received request to build schema")
    cattlelog.debug("Headers: {}".format(request.headers))

    upload_cleanup()
    resp = make_response()

    if request.method == 'POST':
        if 'csv' not in request.files:
            cattlelog.error("No file part")
            return resp, 400
        file = request.files['csv']
        if file.filename == '':
            cattlelog.error('No selected file')
            return resp, 400
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            with open(os.path.join(app.config['UPLOAD_FOLDER'], filename)) as saved_file:
                cattlelog.debug(saved_file.read())
            cattlelog.debug("File {} uploaded successfully".format(file.filename))
            cattlelog.debug("Running COW build")
            cattlelog.debug("Finished with exit status: {}".format(subprocess.call(['cow_tool', 'build', os.path.join(app.config['UPLOAD_FOLDER'], filename)])))
            with open(os.path.join(app.config['UPLOAD_FOLDER'], filename + '-metadata.json')) as json_file:
                json_schema = json.loads(json_file.read())
            resp = make_response(jsonify(json_schema))

    return resp, 200

@app.route('/convert', methods=['POST'])
def convert():
    cattlelog.info("Received request to convert file")
    cattlelog.debug("Headers: {}".format(request.headers))

    upload_cleanup()
    resp = make_response()

    if request.method == 'POST':
        if 'csv' not in request.files or 'json' not in request.files:
            cattlelog.error("Expected a csv and a json file")
            return resp, 400
        file_csv = request.files['csv']
        file_json = request.files['json']
        if file_csv.filename == '' or file_json.filename == '':
            cattlelog.error('No selected file; please send both csv and json file')
            return resp, 400
        if file_csv and file_json and allowed_file(file_csv.filename) and allowed_file(file_json.filename):
            filename_csv = secure_filename(file_csv.filename)
            filename_json = secure_filename(file_json.filename)
            file_csv.save(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv))
            file_json.save(os.path.join(app.config['UPLOAD_FOLDER'], filename_json))
            cattlelog.debug("Files {} and {} uploaded successfully".format(file_csv.filename, file_json.filename))
            cattlelog.debug("Running COW convert")
            cattlelog.debug("Finished with exit status: {}".format(subprocess.call(['cow_tool', 'convert', os.path.join(app.config['UPLOAD_FOLDER'], filename_csv)])))
            with open(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv + '.nq')) as nquads_file:
                g = ConjunctiveGraph()
                g.parse(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv + '.nq'), format='nquads')
            if not request.headers['Accept'] or request.headers['Accept'] == '*/*':
                resp = make_response(g.serialize(format='application/ld+json'))
                resp.headers['Content-Type'] = 'application/ld+json'
            elif request.headers['Accept'] in ACCEPTED_TYPES:
                resp = make_response(g.serialize(format=request.headers['Accept']))
                resp.headers['Content-Type'] = request.headers['Accept']
            else:
                return 'Requested format unavailable', 415

    return resp, 200

if __name__ == '__main__':
    app.run(port=8088, debug=True)

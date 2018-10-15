#!/usr/bin/env python

# cattle: A Web service for COW

from flask import Flask, request, render_template, make_response, redirect, jsonify, session
from werkzeug.utils import secure_filename
import logging
import requests
import os
import subprocess
import json
from rdflib import ConjunctiveGraph
from cow_csvw.csvw_tool import COW
import StringIO
import gzip
import shutil
import traceback
from hashlib import md5
from time import sleep, time
from updateWebhooks import update_webhooks
from string import ascii_uppercase, digits
import random

from druid_integration import druid2cattle, make_hash_folder
from mail_templates import send_new_graph_message

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

AUTH_TOKEN = "xxx"
MAILGUN_AUTH_TOKEN = "yyy"
SECRET_SESSION_KEY = b"zzz"
app.secret_key = SECRET_SESSION_KEY
ERROR_MAIL_ADDRESS = "xyxyxy"

# Util functions

def allowed_file(filename):
	return '.' in filename and \
		   filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def create_random_id(size=10):
	chars = ascii_uppercase + digits
	return ''.join(random.choice(chars) for _ in range(size))

# create cookie for this specific user
# in order to distinquish different json files that originated the same csv file.
def create_user_cookie():
	if 'user_location' in session:
		cattlelog.debug('a file_location is already available %s' % session['user_location'])
	else: #TODO: check if the random id already exists?
		session['user_location'] = create_random_id()

# create cookie for this json file
def create_json_loc_cookie(json_loc):
	if 'file_location' in session:
		cattlelog.debug("deleting the old file_location...")
		session.pop('username', None)
	else:
		cattlelog.debug("No previous file_location was found.")
	cattlelog.debug("creating a new file_location...")
	session['file_location'] = json_loc
	cattlelog.debug("a new file_location has been created: {}".format(session['file_location']))
	# return session['file_location'] #unnecesary

# Routes

@app.route('/', methods=['GET', 'POST'])
def cattle():
	cattlelog.info("Received request to render index")
	if 'file_location' in session:
		try:
			resp = make_response(render_template('index.html', version=subprocess.check_output(['cow_tool', '--version'], stderr=subprocess.STDOUT).strip(), currentFile=os.path.basename(session['file_location'])[:-len("-metadata.json")]))
		except:	
			resp = make_response(render_template('index.html', version='?.??', currentFile=os.path.basename(session['file_location'])[:-len("-metadata.json")]))
	else:
		resp = make_response(render_template('index.html', version=subprocess.check_output(['cow_tool', '--version'], stderr=subprocess.STDOUT).strip(), currentFile=''))
	resp.headers['X-Powered-By'] = 'https://github.com/CLARIAH/cattle'

	return resp

@app.route('/version', methods=['GET', 'POST'])
def version():
	v = subprocess.check_output(['cow_tool', '--version'], stderr=subprocess.STDOUT)
	cattlelog.debug("Version {}".format(v))

	return v

@app.route('/build', methods=['POST'])
def build():
	cattlelog.info("Received request to build schema")
	cattlelog.debug("Headers: {}".format(request.headers))

	create_user_cookie()
	app.config['UPLOAD_FOLDER'] = os.path.join(app.config['UPLOAD_FOLDER'], session['user_location'])

	resp = make_response()

	if 'csv' not in request.files:
		cattlelog.error("No file part")
		return resp, 400
	file = request.files['csv']
	filename = secure_filename(file.filename)
	if filename == '':
		cattlelog.error('No selected file')
		return resp, 400
	if file and allowed_file(filename):
		app.config['UPLOAD_FOLDER'] = make_hash_folder(app.config['UPLOAD_FOLDER'], file)

		if not os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename)):
			file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

		cattlelog.debug("File {} uploaded successfully".format(os.path.join(app.config['UPLOAD_FOLDER'], filename)))
		
		cattlelog.debug("Running COW build")
		COW(mode='build', files=[os.path.join(app.config['UPLOAD_FOLDER'], filename)])
		cattlelog.debug("Build finished")
		with open(os.path.join(app.config['UPLOAD_FOLDER'], filename + '-metadata.json')) as json_file:
			json_schema = json.loads(json_file.read())
		create_json_loc_cookie(os.path.join(app.config['UPLOAD_FOLDER'], filename + '-metadata.json'))
		# resp = make_response(jsonify(json_schema)) #no longer return the json (only to ruminator)
		return cattle()
	else:
		cattlelog.error('No file supplied or wrong file type')
		return resp, 415

	return resp, 200

# @app.route('/convert', methods=['POST'])
# def convert():
# 	cattlelog.info("Received request to convert file")
# 	cattlelog.debug("Headers: {}".format(request.headers))

# 	# upload_cleanup()
# 	resp = make_response()

# 	if 'csv' not in request.files or 'json' not in request.files:
# 		cattlelog.error("Expected a csv and a json file")
# 		return resp, 400
# 	file_csv = request.files['csv']
# 	filename_csv = secure_filename(file_csv.filename)
# 	file_json = request.files['json']
# 	filename_json = secure_filename(file_json.filename)
# 	if filename_csv == '' or filename_json == '':
# 		cattlelog.error('No selected file; please send both csv and json file')
# 		return resp, 400
# 	if file_csv and file_json and allowed_file(filename_csv) and allowed_file(filename_json):
# 		app.config['UPLOAD_FOLDER'] = make_hash_folder(app.config['UPLOAD_FOLDER'], file_csv, file_json)

# 		if not os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv)):
# 			file_csv.save(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv))
# 		if not os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv + '-metadata.json')):
# 			file_json.save(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv + '-metadata.json'))
# 		cattlelog.debug("Files {} and {} uploaded successfully".format(filename_csv, filename_json))

# 		cattlelog.debug("Running COW convert")
# 		COW(mode='convert', files=[os.path.join(app.config['UPLOAD_FOLDER'], filename_csv)])
# 		cattlelog.debug("Convert finished")
# 		try:
# 			with open(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv + '.nq')) as nquads_file:
# 				g = ConjunctiveGraph()
# 				g.parse(nquads_file, format='nquads')
# 		except IOError:
# 			raise IOError("COW could not generate any RDF output. Please check the syntax of your CSV and JSON files and try again.")
# 		if not request.headers['Accept'] or '*/*' in request.headers['Accept']:
# 			if request.form.get('turtle'):
# 				resp = make_response(g.serialize(format='turtle'))
# 				resp.headers['Content-Type'] = 'application/turtle'
# 				resp.headers['Content-Disposition'] = 'attachment; filename=' + filename_csv + '.ttl'
# 			elif request.form.get('zip'): #Requested compressed download
# 				out = StringIO.StringIO()
# 				with gzip.GzipFile(fileobj=out, mode="w") as f:
# 				  f.write(g.serialize(format='application/n-quads'))
# 				resp = make_response(out.getvalue())
# 				resp.headers['Content-Type'] = 'application/gzip'
# 				resp.headers['Content-Disposition'] = 'attachment; filename=' + filename_csv + '.nq.gz'
# 			else:
# 				resp = make_response(g.serialize(format='application/n-quads'))
# 				resp.headers['Content-Type'] = 'application/n-quads'
# 				resp.headers['Content-Disposition'] = 'attachment; filename=' + filename_csv + '.nq'
# 		elif request.headers['Accept'] in ACCEPTED_TYPES:
# 			resp = make_response(g.serialize(format=request.headers['Accept']))
# 			resp.headers['Content-Type'] = request.headers['Accept']
# 		else:
# 			return 'Requested format unavailable', 415
# 	else:
# 		raise Exception('No files supplied, wrong file types, or unexpected file extensions')

# 	return resp, 200

@app.route('/convert_local', methods=['POST'])
def convert_local():
	cattlelog.info("Received request to convert files locally")
	cattlelog.debug("Headers: {}".format(request.headers))

	resp = make_response()

	filename_csv = os.path.basename(session['file_location'])[:-len('-metadata.json')]
	filename_json = os.path.basename(session['file_location'])
	app.config['UPLOAD_FOLDER'] = os.path.dirname(session['file_location'])

	if os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv)) and os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename_json)):
		cattlelog.debug("Running COW convert")
		COW(mode='convert', files=[os.path.join(app.config['UPLOAD_FOLDER'], filename_csv)])
		cattlelog.debug("Convert finished")
		try:
			with open(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv + '.nq')) as nquads_file:
				g = ConjunctiveGraph()
				g.parse(nquads_file, format='nquads')
		except IOError:
			raise IOError("COW could not generate any RDF output. Please check the syntax of your CSV and JSON files and try again.")
		if not request.headers['Accept'] or '*/*' in request.headers['Accept']:
			if request.form.get('turtle'):
				resp = make_response(g.serialize(format='turtle'))
				resp.headers['Content-Type'] = 'application/turtle'
				resp.headers['Content-Disposition'] = 'attachment; filename=' + filename_csv + '.ttl'
			elif request.form.get('zip'): #Requested compressed download
				out = StringIO.StringIO()
				with gzip.GzipFile(fileobj=out, mode="w") as f:
				  f.write(g.serialize(format='application/n-quads'))
				resp = make_response(out.getvalue())
				resp.headers['Content-Type'] = 'application/gzip'
				resp.headers['Content-Disposition'] = 'attachment; filename=' + filename_csv + '.nq.gz'
			else:
				resp = make_response(g.serialize(format='application/n-quads'))
				resp.headers['Content-Type'] = 'application/n-quads'
				resp.headers['Content-Disposition'] = 'attachment; filename=' + filename_csv + '.nq'
		elif request.headers['Accept'] in ACCEPTED_TYPES:
			resp = make_response(g.serialize(format=request.headers['Accept']))
			resp.headers['Content-Type'] = request.headers['Accept']
		else:
			return 'Requested format unavailable', 415
	else:
		raise Exception('No files supplied, wrong file types, or unexpected file extensions')

	return resp, 200

@app.route('/druid/<username>/<dataset>', methods=['POST'])
def druid(username, dataset):
	# Retrieves a list of Druid files in a dataset; if .csv and .json present, downloads them, converts them, uploads results
	# if only a csv is present a json will be created for it.
	cattlelog.debug("Starting Druid-based conversion")

	try:
		request_json = request.json
		if request.json['assets'][0]['assetName'].endswith('.csv'):
			cattlelog.debug("Waiting for possible json-files...")
			sleep(10) #wait for possible json files to be uploaded.
	except:
		pass

	resp = make_response()

	d2c = druid2cattle(username, dataset, cattlelog, app.config['UPLOAD_FOLDER'], requests, AUTH_TOKEN)
	candidates, singles = d2c.get_candidates()

	successes = []
	if len(candidates.keys()) > 0:
		successes += d2c.handle_pairs(candidates)
	if len(singles.keys()) > 0:
		successes += d2c.handle_singles(singles)
	try:
		email_address = request.json['user']['email']
		account_name = request.json['user']['accountName']
		if len(successes) > 0:
			send_new_graph_message(email_address, account_name, successes, MAILGUN_AUTH_TOKEN)
	except:
		pass
	return resp, 200

@app.route('/ruminator', methods=['GET', 'POST'])
def ruminator():
	if 'file_location' in session:
		file_location = session['file_location']
		with open(file_location) as json_file:
			return render_template('ruminator.html', json_contents=json_file.read())
	else:
		return render_template('ruminator.html', file_location=0)

@app.route('/save_json', methods=['POST'])
def save_json():
	jsdata = request.form['javascript_data']
	with open(session['file_location'], 'w') as json_file:
		json_file.write(jsdata)
	cattlelog.debug("The json file has been altered and saved. :D")
	resp = make_response()
	return resp, 200

# @app.route('/clean_session', methods=['GET', 'POST'])
# def clean_session():
	

@app.route('/webhook_shooter', methods=['GET', 'POST'])
def webhook_shooter():
	update_webhooks("Cattle", AUTH_TOKEN)
	cattlelog.debug("Webhook_shooter was called!")
	return render_template('webhook.html')

# Error handlers

@app.errorhandler(404)
def pageNotFound(error):
	return render_template('error.html', error_message="Page not found", error_mail_address=ERROR_MAIL_ADDRESS)

@app.errorhandler(500)
def pageNotFound(error):
	return render_template('error.html', error_message=error.message, error_mail_address=ERROR_MAIL_ADDRESS)

if __name__ == '__main__':
	app.run(port=8088, debug=False)

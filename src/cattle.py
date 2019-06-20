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
import io

from druid_integration import druid2cattle, make_hash_folder
from druid_longer import remove_files
import info_log

# The Flask app
app = Flask(__name__)

# Uploading
UPLOAD_FOLDER_BASE = '/home/cattle/storage'
ALLOWED_EXTENSIONS = set(['csv', 'json'])
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER_BASE

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

EXTENSION_DICT = {"n3": ".n3",
	"nquads": ".nq",
	"nt": ".nt",
	"rdfxml": ".rdf",
	"trig": ".trig",
	"trix": ".xml",
	"turtle": ".ttl",
	"xml": ".rdf",
	"json-ld": ".jsonld"}

MIME_TYPE_DICT = {"n3": "text/n3",
	"nquads": "application/n-quads",
	"nt": "application/n-triples",
	"rdfxml": "application/rdf+xml",
	"trig": "application/trig",
	"trix": "application/xml",
	"turtle": "text/turtle",
	"xml": "application/rdf+xml",
	"json-ld": "application/ld+json"} 

AUTH_TOKEN = "xxx"
SECRET_SESSION_KEY = b"zzz"
app.secret_key = SECRET_SESSION_KEY
ERROR_MAIL_ADDRESS = "xyxyxy"

# Util functions

def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def create_random_id(size=10):
	chars = ascii_uppercase + digits
	return ''.join(random.choice(chars) for _ in range(size))

# create cookie for this specific user
# in order to distinquish different json files that originated the same csv file.
def create_user_cookie():
	if 'user_location' in session:
		cattlelog.debug('a user directory is already available %s' % session['user_location'])
	else: #TODO: check if the random id already exists?
		session['user_location'] = create_random_id()

# create cookie for this json file
def create_json_loc_cookie(json_loc):
	if 'file_location' in session:
		cattlelog.debug("deleting the old file_location...")
		clean_session()
	else:
		cattlelog.debug("No previous file_location was found.")
	cattlelog.debug("creating a new file_location...")
	session['file_location'] = json_loc
	cattlelog.debug("a new file_location has been created: {}".format(session['file_location']))
	# return session['file_location'] #unnecesary

def clean_session():
	session.pop('file_location', None)
	app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER_BASE

def upload_files():
	cattlelog.info("Uploading csv and json files...")
	create_user_cookie()
	app.config['UPLOAD_FOLDER'] = os.path.join(app.config['UPLOAD_FOLDER'], session['user_location'])

	if 'csv' in request.files and 'json' in request.files:
		csv_file = request.files['csv']
		json_file= request.files['json']
		csv_filename = secure_filename(csv_file.filename)
		json_filename = secure_filename(json_file.filename)

		if csv_filename == '' or json_filename == '':
			cattlelog.error('No selected file')
			return 0

		if csv_file and json_file and allowed_file(csv_filename) and allowed_file(json_filename):
			app.config['UPLOAD_FOLDER'] = make_hash_folder(app.config['UPLOAD_FOLDER'], csv_file, json_file)

			if not os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], csv_filename)):
				csv_file.save(os.path.join(app.config['UPLOAD_FOLDER'], csv_filename))
			if not os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], json_filename)):
				json_file.save(os.path.join(app.config['UPLOAD_FOLDER'], json_filename))

			cattlelog.debug("Files {} and {} uploaded successfully".format(os.path.join(app.config['UPLOAD_FOLDER'], csv_filename),os.path.join(app.config['UPLOAD_FOLDER'], json_filename)))
	else:
		return 0

	create_json_loc_cookie(os.path.join(app.config['UPLOAD_FOLDER'], json_filename))
	cattlelog.info("Upload complete.")

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
def build(internal=False):
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

		infolog = info_log.info_log(os.path.join(app.config['UPLOAD_FOLDER'], filename))
		cattlelog.debug("info_log_path= {}".format(os.path.join(app.config['UPLOAD_FOLDER'], filename)))
		infolog.job_start("build")
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
		# return cattle()
		infolog.job_end("build")
		app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER_BASE
		if not internal:
			return render_template('build.html', currentFile=os.path.basename(session['file_location'])[:-len("-metadata.json")])
		else:
			return 0
	else:
		cattlelog.error('No file supplied or wrong file type')
		return resp, 415

	return resp, 200

@app.route('/convert_local', methods=['POST'])
def convert_local():
	cattlelog.info("Received request to convert files locally")
	cattlelog.debug("Headers: {}".format(request.headers))

	filename_csv = os.path.basename(session['file_location'])[:-len('-metadata.json')]
	filename_json = os.path.basename(session['file_location'])
	app.config['UPLOAD_FOLDER'] = os.path.dirname(session['file_location'])

	infolog = info_log.info_log(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv))
	infolog.job_start("convert")

	if os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv)) and os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename_json)):
		cattlelog.debug("Running COW convert")
		infolog.job_start("cow - conversion")
		# sub_log_name = infolog.sub_start("cow - conversion")
		# with open(os.path.join(app.config['UPLOAD_FOLDER'], sub_log_name), 'w') as sub_log:
		# 	cattlelog.debug(os.path.join(app.config['UPLOAD_FOLDER'], sub_log_name))
		# 	subprocess.Popen(["python", "src/cattle_longer.py", "-path", os.path.join(app.config['UPLOAD_FOLDER'], filename_csv)], stdout=sub_log, stderr=subprocess.STDOUT)
		COW(mode='convert', files=[os.path.join(app.config['UPLOAD_FOLDER'], filename_csv)])
		infolog.job_end("cow - conversion")
		cattlelog.debug("Convert finished")
		try:
			with open(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv + '.nq')) as nquads_file:
				g = ConjunctiveGraph()
				g.parse(nquads_file, format='nquads')
		except IOError:
			raise IOError("COW could not generate any RDF output. Please check the syntax of your CSV and JSON files and try again.")
		if not request.headers['Accept'] or '*/*' in request.headers['Accept']:
			if request.form.get('zip'): #Requested compressed download
				out = StringIO.StringIO()
				with gzip.GzipFile(fileobj=out, mode="w") as f:
				  f.write(g.serialize(format=request.form.get('formatSelect')))
				resp = make_response(out.getvalue())
				resp.headers['Content-Type'] = 'application/gzip'
				resp.headers['Content-Disposition'] = 'attachment; filename=' + filename_csv + EXTENSION_DICT[request.form.get('formatSelect')] + '.gz'
			else:
				resp = make_response(g.serialize(format=request.form.get('formatSelect')))
				resp.headers['Content-Type'] = MIME_TYPE_DICT[request.form.get('formatSelect')]
				resp.headers['Content-Disposition'] = 'attachment; filename=' + filename_csv + EXTENSION_DICT[request.form.get('formatSelect')]
		elif request.headers['Accept'] in ACCEPTED_TYPES:
			resp = make_response(g.serialize(format=request.headers['Accept']))
			resp.headers['Content-Type'] = request.headers['Accept']
		else:
			return 'Requested format unavailable', 415
	else:
		raise Exception('No files supplied, wrong file types, or unexpected file extensions')

	remove_files(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv))
	clean_session()
	infolog.job_end("convert")
	return resp, 200

@app.route('/build_convert', methods=['GET', 'POST'])
def build_convert():
	if 'json' not in request.files:
		build(True)
	else:
		cattlelog.debug("found a json!:")
		cattlelog.debug(request.files['json'])
		upload_files()

	return convert_local()

@app.route('/convert', methods=['GET', 'POST'])
def convert():
	if 'file_location' in session:
		return render_template('convert.html', currentFile=os.path.basename(session['file_location'])[:-len("-metadata.json")])
	else:
		return render_template('convert.html', currentFile="")

@app.route('/druid/<username>/<dataset>', methods=['POST'])
def druid(username, dataset):
	# Retrieves a list of Druid files in a dataset; if .csv and .json present, downloads them, converts them, uploads results
	# if only a csv is present a json will be created for it.
	cattlelog.debug("Starting Druid-based conversion")

	try:
		if request.json['assets'][0]['assetName'].endswith('.csv'):
			cattlelog.debug("Waiting for possible json-files...")
			sleep(10) #wait for possible json files to be uploaded.
	except:
		pass

	resp = make_response()

	# cattlelog.debug("UPLOAD_FOLDER=== {}".format(app.config['UPLOAD_FOLDER']))
	d2c = druid2cattle(username, dataset, cattlelog, app.config['UPLOAD_FOLDER'], requests, AUTH_TOKEN)
	candidates, singles = d2c.get_candidates()
 	#################
	try: ####UGLY FOR TESTING!!!!!!!!!!
		basename = request.json['assets'][0]['assetName'][:request.json['assets'][0]['assetName'].find(".csv")+4]
	except:
		basename = "imf.csv" 
	candidates, singles = d2c.select_candidate(candidates, singles, basename)

	if len(candidates.keys()) > 0:
		d2c.handle_pairs(candidates)
	if len(singles.keys()) > 0:
		d2c.handle_singles(singles)

	return resp, 200

@app.route('/ruminator', methods=['GET', 'POST'])
def ruminator():
	if 'file_location' in session:
		file_location = session['file_location']
		with open(file_location) as json_file:
			return render_template('ruminator.html', json_contents=json_file.read())
	else:
		return render_template('ruminator.html', json_contents={})

@app.route('/save_json', methods=['POST'])
def save_json():
	jsdata = request.form['javascript_data']
	with io.open(session['file_location'], 'w') as json_file:
		json_file.write(jsdata)
	cattlelog.debug("The json file has been altered and saved. :D")
	resp = make_response()
	return resp, 200

@app.route('/webhook_shooter', methods=['GET', 'POST'])
def webhook_shooter():
	update_webhooks("Cattle", AUTH_TOKEN)
	cattlelog.debug("Webhook_shooter was called!")
	return render_template('webhook.html')

@app.route('/info', methods=['GET'])
@app.route('/info/<username>/<dataset>', methods=['GET'])
def info(username=None, dataset=None):
	if username == None:
		try:
			username = session['user_location']
			dataset='web_interface'
		except:
			cattlelog.debug("NO USERNAME WAS FOUND!!!")
			return render_template('info_spec.html', log_data={"no user has been specified, so there isn't any information available.": [{}]})
	total_log = info_log.get_combined_log(UPLOAD_FOLDER_BASE)
	# cattlelog.debug(json.dumps(total_log[username][dataset], indent=4))
	# return render_template('info.html', log_data=total_log)
	try:
		return render_template('info_spec.html', log_data=total_log[username][dataset])
	except:
		return render_template('info_spec.html', log_data={"no user has been specified, so there isn't any information available.": [{}]})

# Error handlers

@app.errorhandler(404)
def pageNotFound(error):
	return render_template('error.html', error_message="Page not found", error_mail_address=ERROR_MAIL_ADDRESS)

@app.errorhandler(500)
def pageNotFound(error):
	return render_template('error.html', error_message=error.message, error_mail_address=ERROR_MAIL_ADDRESS)

if __name__ == '__main__':
	app.run(port=8088, debug=False)

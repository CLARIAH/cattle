#!/usr/bin/env python

# cattle: A Web service for COW

from flask import Flask, request, render_template, make_response, redirect, jsonify, session, send_from_directory
from werkzeug.utils import secure_filename
import logging
import requests
import os
import subprocess
import json
from rdflib import ConjunctiveGraph
from cow_csvw.csvw_tool import COW
from io import BytesIO #changed for python3
import gzip
import shutil
import traceback
from string import ascii_uppercase, digits
import random
import io

from hash_folder import make_hash_folder
from cattle_process import create_thread

# The Flask app
app = Flask(__name__)

# Uploading
UPLOAD_FOLDER_BASE = '/tmp'
ALLOWED_EXTENSIONS = set(['csv', 'json', 'tsv'])
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

SECRET_SESSION_KEY = b"zzz"
app.secret_key = SECRET_SESSION_KEY
ERROR_MAIL_ADDRESS = "xyxyxy"

# Util functions

def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# returns a string of random letters and numbers.
def create_random_id(size=10):
	chars = ascii_uppercase + digits
	return ''.join(random.choice(chars) for _ in range(size))

# add a random id to identify this specific user in order to distinquish 
# different json files that originated from the same csv file.
def create_user_cookie():
	if 'user_location' in session:
		cattlelog.debug('a user directory is already available %s' % session['user_location'])
	else:
		session['user_location'] = str(create_random_id())

# add the location of the json file to the current session.
def create_json_loc_cookie(json_loc):
	if 'file_location' in session:
		cattlelog.debug("deleting the old file_location...")
		clean_session()
	else:
		cattlelog.debug("No previous file_location was found.")
	session['file_location'] = str(json_loc)
	cattlelog.debug("a new file_location has been created: {}".format(session['file_location']))

# delete a file located at a given location.
def delete_file(path):
	path_to, filename = os.path.split(path)
	try:
		os.remove(path)
		cattlelog.debug("Finished removing: {}".format(filename))
	except:
		cattlelog.debug("Cattle was not able to delete \"{}\"".format(filename))

# remove all the user specific information form the session.
def clean_session():
	session.pop('file_location', None)
	session.pop('user_location', None)
	app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER_BASE

# used to upload csv and json file when both are uploaded.
def upload_files():
	cattlelog.info("Uploading csv and json files...")
	delete_data() #delete old data if there is any
	create_user_cookie()
	path = os.path.join(UPLOAD_FOLDER_BASE, session['user_location'])

	if 'csv' in request.files and 'json' in request.files:
		csv_file = request.files['csv']
		json_file= request.files['json']
		csv_filename = secure_filename(csv_file.filename)
		json_filename = secure_filename(json_file.filename)
		if csv_filename.endswith('.tsv'):
			csv_filename = csv_filename[:-3]+"csv"
		if json_filename.endswith("tsv-metadata.json"):
			json_filename = json_filename[:-17]+"csv-metadata.json"

		if csv_filename == '' or json_filename == '':
			cattlelog.error('No selected file')
			return 0

		if csv_file and json_file and allowed_file(csv_filename) and allowed_file(json_filename):
			path = make_hash_folder(path, csv_file, json_file)

			if not os.path.exists(os.path.join(path, csv_filename)):
				csv_file.save(os.path.join(path, csv_filename))
			if not os.path.exists(os.path.join(path, json_filename)):
				json_file.save(os.path.join(path, json_filename))

			cattlelog.debug("Files {} and {} uploaded successfully".format(os.path.join(path, csv_filename),os.path.join(path, json_filename)))
	else:
		return 0

	create_json_loc_cookie(os.path.join(path, json_filename))
	cattlelog.info("Upload complete.")

# Routes

# returns the main index of cattle.
@app.route('/', methods=['GET', 'POST'])
def cattle():
	cattlelog.info("Received request to render index")
	if 'file_location' in session:
		try: # in some (offline) cases there was a problem with retrieving the version of cow, this try/except removes that problem.
			resp = make_response(render_template('index.html', version=(subprocess.check_output(['cow_tool', '--version'], stderr=subprocess.STDOUT)).decode(), currentFile=os.path.basename(session['file_location'])[:-len("-metadata.json")]))
		except:	
			resp = make_response(render_template('index.html', version='?.??', currentFile=os.path.basename(session['file_location'])[:-len("-metadata.json")]))
	else:
		try:
			resp = make_response(render_template('index.html', version=(subprocess.check_output(['cow_tool', '--version'], stderr=subprocess.STDOUT)).decode(), currentFile=''))
		except:	
			resp = make_response(render_template('index.html', version='?.??', currentFile=''))

	resp.headers['X-Powered-By'] = 'https://github.com/CLARIAH/cattle'

	return resp

@app.route('/version', methods=['GET', 'POST'])
def version():
	v = subprocess.check_output(['cow_tool', '--version'], stderr=subprocess.STDOUT)
	cattlelog.debug("Version {}".format(v))

	return v

# creates a json scheme from an uploaded csv file.
@app.route('/build', methods=['POST'])
def build():
	cattlelog.info("Received request to build schema")
	cattlelog.debug("Headers: {}".format(request.headers))

	delete_data() #delete old data if there is any
	create_user_cookie()
	cattlelog.debug("type of this session object: {}".format(type(session['user_location'])))
	path = os.path.join(UPLOAD_FOLDER_BASE , str(session['user_location']))

	resp = make_response()

	if 'csv' not in request.files:
		cattlelog.error("No file part")
		return resp, 400
	file = request.files['csv']
	filename = secure_filename(file.filename)
	if filename.endswith('.tsv'):
		filename = filename[:-3]+"csv"
	if filename == '':
		cattlelog.error('No selected file')
		return resp, 400
	if file and allowed_file(filename):
		path = make_hash_folder(path, file)
		if not os.path.exists(os.path.join(path, filename)):
			file.save(os.path.join(path, filename))
		cattlelog.debug("File {} uploaded successfully".format(os.path.join(path, filename)))
		
		cattlelog.debug("Running COW build")
		COW(mode='build', files=[os.path.join(path, filename)])
		cattlelog.debug("Build finished")
		with open(os.path.join(path, filename + '-metadata.json')) as json_file:
			json_schema = json.loads(json_file.read())
		create_json_loc_cookie(os.path.join(path, filename + '-metadata.json'))
		return render_template('decide_scheme.html', currentFile=os.path.basename(session['file_location'])[:-len("-metadata.json")])
	else:
		cattlelog.error('No file supplied or wrong file type')
		return resp, 415

# starts the thread where the csv is converted into linked data with COW
# and returns the download page to the user.
@app.route('/convert', methods=['POST', 'GET'])
def convert():
	cattlelog.info("Received request to convert files locally")
	# cattlelog.debug("Headers: {}".format(request.headers))

	app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER_BASE #because otherwise it nests after cattle errors

	filename_csv = os.path.basename(session['file_location'])[:-len('-metadata.json')]
	filename_json = os.path.basename(session['file_location'])
	path = os.path.dirname(session['file_location'])

	if os.path.exists(os.path.join(path, filename_csv)) and os.path.exists(os.path.join(path, filename_json)):
		cattlelog.debug("Running COW convert")
		cattlelog.debug("The size of the file is {} Bytes.".format(os.stat(os.path.join(path, filename_csv)).st_size))
		create_thread(os.path.join(path, filename_csv), cattlelog)
		path_list = path.split(os.sep)
		cattlelog.debug("path_list: {}".format(path_list))
		return download_page(path_list[-3] + "." + path_list[-1])
	else:
		raise Exception('No files supplied, wrong file types, or unexpected file extensions')

# if only a csv (no json) file is uploaded a new json scheme will
# be created with the uploaded csv file, if both a csv and json
# were uploaded cattle immediatly converts them into linked data
# and returns the download page.
@app.route('/build_convert', methods=['GET', 'POST'])
def build_convert():
	if 'json' not in request.files:
		return build()
	else:
		cattlelog.debug("found a json!:")
		cattlelog.debug(request.files['json'])
		upload_files()
		return convert()

# returns an instance of ruminator with the json file 
# created using COW.
@app.route('/ruminator', methods=['GET', 'POST'])
def ruminator():
	if 'file_location' in session:
		file_location = session['file_location']
		with open(file_location) as json_file:
			return render_template('ruminator.html', json_contents=json_file.read())
	else:
		return render_template('ruminator.html', json_contents={})

# used by ruminator to save the changes made to the json scheme.
@app.route('/save_json', methods=['POST'])
def save_json():
	jsdata = request.form['javascript_data']
	with io.open(session['file_location'], 'w') as json_file:
		json_file.write(jsdata)
	cattlelog.debug("The json file has been altered and saved. :D")
	resp = make_response()
	return resp, 200

# returns the page where the linked data can be downloaded when the 
# conversion by COW is finished, before then it returns a page asking 
# for the users patience.
@app.route('/download/<combined_hash>')
def download_page(combined_hash):
	cattlelog.debug("this is the hash: {}".format(combined_hash))
	user_hash, file_hash = combined_hash.split('.')
	file_location = os.path.join(UPLOAD_FOLDER_BASE, user_hash, 'web_interface', file_hash)
	try:
		csv_files = [f for f in os.listdir(file_location) if f.endswith(".csv")]
		json_files = [f for f in os.listdir(file_location) if f.endswith(".json")]
	except:
		return render_template('error.html', error_message="This hash [{}] does not resolve to a file.".format(combined_hash), error_mail_address=ERROR_MAIL_ADDRESS, currentFile="")
	if len(csv_files) < 1 and len(json_files) < 1: 
		return render_template('download_page.html', ready_for_download=True, hash=combined_hash, currentFile=os.path.basename(session['file_location'])[:-len("-metadata.json")])
	else:
		cattlelog.debug("combined hash: {}".format(combined_hash))
		return render_template('download_page.html', ready_for_download=False, hash=combined_hash, currentFile="")

# returns the linked data in the user specified format to the user.
@app.route('/download_/<combined_hash>', methods=['POST'])
def download_linked_data(combined_hash):
	cattlelog.debug("the hash: {}".format(combined_hash))
	user_hash, file_hash = combined_hash.split('.')
	file_location = os.path.join(UPLOAD_FOLDER_BASE, user_hash, 'web_interface', file_hash)

	try:
		rdf_file = [f for f in os.listdir(file_location) if f.endswith(".csv.nq")]
	except:
		return render_template('error.html', error_message="This hash [{}] does not resolve to a file.".format(combined_hash), error_mail_address=ERROR_MAIL_ADDRESS, currentFile="")

	rdf_file = rdf_file[0]
	filename_csv = rdf_file[:-3]
	try:
		g = ConjunctiveGraph()
		g.parse(location=os.path.join(file_location, rdf_file), format='nquads')
	except IOError:
		raise IOError("COW could not generate any RDF output. Please check the syntax of your CSV and JSON files and try again.")
	if not request.headers['Accept'] or '*/*' in request.headers['Accept']:
		if request.form.get('zip'): #Requested compressed download
			out = BytesIO()
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

	try:
		return resp, 200
	except Exception as e:
		return render_template('error.html', error_message="Cattle was not able to find your linked data file. Error Message: {}".format(e), error_mail_address=ERROR_MAIL_ADDRESS, currentFile="")


@app.route('/download_json', methods=['GET'])
def download_json():
	if 'file_location' in session:
		path, filename = os.path.split(session['file_location'])
		try:
			return send_from_directory(path, filename, as_attachment=True)
		except Exception as e:
			return render_template('error.html', error_message="Cattle was not able to find your json file. Error Message: {}".format(e), error_mail_address=ERROR_MAIL_ADDRESS, currentFile="")

# used to upload a new json scheme and replace the previously build json scheme.
@app.route('/upload_json', methods=['GET', 'POST'])
def upload_json(): #expects a session key is already available, and json already exists
	cattlelog.info("Uploading json file...")
	path = os.path.join(UPLOAD_FOLDER_BASE, session['user_location'])

	if 'json' in request.files:
		json_file = request.files['json']

		if json_file:
			filepath = session['file_location']
			os.remove(filepath)
			json_file.save(filepath)

			cattlelog.debug("new json uploaded successfully")
		return convert()
	else:
		cattlelog.debug("ERROR: could not find the new json file.")
		return 0

@app.route('/manual_scheme', methods=['GET', 'POST'])
def manual_scheme():
	if 'file_location' in session:
		return render_template('manual_scheme.html', currentFile=os.path.basename(session['file_location'])[:-len("-metadata.json")])
	else:
		return render_template('manual_scheme.html', currentFile="")

@app.route('/delete_data', methods=['GET', 'POST'])
def delete_data():
	try:
		json_path = session['file_location']
	except Exception as e:
		return render_template('error.html', error_message="There does not seem to be any data to remove. Error Message: {}".format(e), error_mail_address=ERROR_MAIL_ADDRESS, currentFile="") 

	delete_file(json_path[:-len("-metadata.json")]) #removes csv files
	delete_file(json_path) #removes json file
	delete_file(json_path[:-len("-metadata.json")] + ".nq") #removes nquats file

	clean_session()

	return render_template('delete_data.html', currentFile="")

# Error handlers

@app.errorhandler(404)
def pageNotFound(error):
	return render_template('error.html', error_message="Page not found", error_mail_address=ERROR_MAIL_ADDRESS, currentFile="")

@app.errorhandler(500)
def pageNotFound(error):
	return render_template('error.html', error_message=error, error_mail_address=ERROR_MAIL_ADDRESS, currentFile="")

if __name__ == '__main__':
	app.run(port=8088, debug=False)

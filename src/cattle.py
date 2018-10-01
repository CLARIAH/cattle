#!/usr/bin/env python

# cattle: A Web service for COW

from flask import Flask, request, render_template, make_response, redirect, jsonify
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

from druid_integration import druid2cattle, make_hash_folder
# from mail_templates import send_new_graph_message

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

# Util functions

def allowed_file(filename):
	return '.' in filename and \
		   filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# def upload_cleanup(folder_name): #shouldn't be used anymore!
# 	files = os.listdir(app.config['UPLOAD_FOLDER'])
# 	for file in files:
# 		if file.endswith(".csv") or file.endswith(".json") or file.endswith(".nq") or '-metadata.' in file:
# 			os.remove(os.path.join(app.config['UPLOAD_FOLDER'], folder_name, file))

# Routes

@app.route('/', methods=['GET', 'POST'])
def cattle():
	cattlelog.info("Received request to render index")
	resp = make_response(render_template('index.html', version=subprocess.check_output(['cow_tool', '--version'], stderr=subprocess.STDOUT).strip()))
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

	# upload_cleanup()
	resp = make_response()

	if 'csv' not in request.files:
		cattlelog.error("No file part")
		return resp, 400
	file = request.files['csv']
	if file.filename == '':
		cattlelog.error('No selected file')
		return resp, 400
	if file and allowed_file(file.filename):
		filename = secure_filename(file.filename)
		app.config['UPLOAD_FOLDER'] = make_hash_folder(app.config['UPLOAD_FOLDER'], file)

		if not os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename)):
			file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
		#   with open(os.path.join(app.config['UPLOAD_FOLDER'], filename)) as saved_file:
		#        cattlelog.debug(saved_file.read())
		cattlelog.debug("File {} uploaded successfully".format(os.path.join(app.config['UPLOAD_FOLDER'], file.filename)))
		cattlelog.debug("Running COW build")
		# try:
		#     ret = subprocess.check_output("cow_tool build {}".format(os.path.join(app.config['UPLOAD_FOLDER'], filename)), stderr=subprocess.STDOUT, shell=True)
		# except subprocess.CalledProcessError as e:
		#     cattlelog.error("COW returned error status: {}".format(e.output))
		#     return make_response(e.output), 200

		COW(mode='build', files=[os.path.join(app.config['UPLOAD_FOLDER'], file.filename)])
		cattlelog.debug("Build finished")
		with open(os.path.join(app.config['UPLOAD_FOLDER'], file.filename + '-metadata.json')) as json_file:
			json_schema = json.loads(json_file.read())
		resp = make_response(jsonify(json_schema))
	else:
		cattlelog.error('No file supplied or wrong file type')
		return resp, 415

	return resp, 200

@app.route('/convert', methods=['POST'])
def convert():
	cattlelog.info("Received request to convert file")
	cattlelog.debug("Headers: {}".format(request.headers))

	# upload_cleanup()
	resp = make_response()

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
		app.config['UPLOAD_FOLDER'] = make_hash_folder(app.config['UPLOAD_FOLDER'], file_csv, file_json)

		if not os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv)):
			file_csv.save(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv))
		if not os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv + '-metadata.json')):
			file_json.save(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv + '-metadata.json'))
		cattlelog.debug("Files {} and {} uploaded successfully".format(file_csv.filename, file_json.filename))
		cattlelog.debug("Running COW convert")
		# try:
		#     ret = subprocess.check_output("cow_tool convert {}".format(os.path.join(app.config['UPLOAD_FOLDER'], filename_csv)), stderr=subprocess.STDOUT, shell=True)
		# except subprocess.CalledProcessError as e:
		#     cattlelog.error("COW returned error status: {}".format(e.output))
		#     return make_response(e.output), 200
		COW(mode='convert', files=[os.path.join(app.config['UPLOAD_FOLDER'], file_csv.filename)])
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

	# cattlelog.debug("<<<<information from the webhook:")
	try:
		request_json = request.json
		if request.json['assets'][0]['assetName'].endswith('.csv'):
			cattlelog.debug("Waiting for possible json-files.")
			sleep(10) #wait for possible json files to be uploaded.
	except:
		pass

	# cattlelog.debug(request.json)
	# cattlelog.debug("end of imformation from the webhook.>>>>")

	# upload_cleanup()
	resp = make_response()

	d2c = druid2cattle(username, dataset, cattlelog, app.config['UPLOAD_FOLDER'], requests, AUTH_TOKEN)

	candidates, singles = d2c.get_candidates()

	successes = []
	if len(candidates.keys()) > 0:
		successes += d2c.handle_pairs(candidates)
	if len(singles.keys()) > 0:
		successes += d2c.handle_singles(singles)

	try:
		email_adress = request.json['user']['email']
		account_name = request.json['user']['accountName']
		cattlelog.debug("information needed for the email!")
		cattlelog.debug(email_adress)
		cattlelog.debug(successes)
		# if len(successes) > 0:
		# 	send_new_graph_message(email_adress, account_name, successes, MAILGUN_AUTH_TOKEN)
	except:
		pass

	return resp, 200

@app.route('/webhook_shooter', methods=['GET', 'POST'])
def webhook_shooter():
# # 	# update_webhooks("Cattle", AUTH_TOKEN)
	cattlelog.debug("Webhook_shooter was called!")
	return render_template('webhook.html')
# 	resp = make_response()
# 	return resp

# Error handlers

@app.errorhandler(404)
def pageNotFound(error):
	return render_template('error.html', error_message="Page not found")

@app.errorhandler(500)
def pageNotFound(error):
	return render_template('error.html', error_message=error.message)

if __name__ == '__main__':
	app.run(port=8088, debug=False)

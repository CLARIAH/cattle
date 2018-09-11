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

# Util functions

def allowed_file(filename):
	return '.' in filename and \
		   filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# def upload_cleanup(folder_name): #shouldn't be used anymore!
# 	files = os.listdir(app.config['UPLOAD_FOLDER'])
# 	for file in files:
# 		if file.endswith(".csv") or file.endswith(".json") or file.endswith(".nq") or '-metadata.' in file:
# 			os.remove(os.path.join(app.config['UPLOAD_FOLDER'], folder_name, file))

#create hash to use as folder to save the files
def create_hash(csv_file, json_file, read_files=True):
	m = md5()
	if read_files:
		if json_file == None:
			m.update(csv_file.read())
			csv_file.seek(0)
		else:
			m.update(csv_file.read() + json_file.read())
			csv_file.seek(0)
			json_file.seek(0)
	else:
		m.update(csv_file + json_file)
	return m.hexdigest()

def make_hash_folder(csv_file, json_file=None):
	hash_folder_name = create_hash(csv_file, json_file)
	app.config['UPLOAD_FOLDER'] = os.path.join(app.config['UPLOAD_FOLDER'], "web_interface", hash_folder_name)
	try:
		os.makedirs(app.config['UPLOAD_FOLDER'])
	except:
		cattlelog.debug("This folder already exists, this might result in concurrency problems.")

def make_hash_folder_druid(csv_string, username, dataset, json_string=""):
	hash_folder_name = create_hash(csv_string, json_string, False)
	try:
		os.makedirs(os.path.join(app.config['UPLOAD_FOLDER'], username, dataset, hash_folder_name))
	except:
		cattlelog.debug("This folder already exists, this might result in concurrency problems.")
	return os.path.join(username, dataset, hash_folder_name)

def make_hash_path_druid(csv_string, username, dataset, json_string=""):
	hash_folder_name = create_hash(csv_string, json_string, False)
	return os.path.join(username, dataset, hash_folder_name)

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
		make_hash_folder(file)

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
		make_hash_folder(file_csv, file_json)

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

### Druid interface
class druid2cattle:
	def __init__(self, username, dataset):
		self.username = username
		self.dataset = dataset

		#changes every time
		self.path = ""

	#retrieves all the .csv-files and .csv- .json-file pairs from a specific druid dataset.
	def get_candidates(self):
		cattlelog.debug("Listing remote files in Druid for user {} dataset {}".format(self.username, self.dataset))
		r = requests.get("https://api.druid.datalegend.net/datasets/{}/{}/assets".format(self.username, self.dataset))
		cattlelog.debug("Remote files are: {}".format(r.json()))
		files = json.loads(r.text)
		url_dict = {}
		for f in files: url_dict[f['assetName']] = f['url']
		duos = {}
		single_csv = {}
		for f in url_dict.keys():
			if f.endswith('.csv') and (f + '-metadata.json') in url_dict.keys():
				duos[f] = (url_dict[f], url_dict[f + '-metadata.json'])
			elif f.endswith('.csv'):
				single_csv[f] = url_dict[f]

		return duos, single_csv

	#returns True if the hash folder already exists and has been modified in the last 60 seconds.
	def check_for_concurrency(self):
		csv_path = os.path.join(app.config['UPLOAD_FOLDER'], self.path)
		if os.path.exists(csv_path) and (time() - os.path.getmtime(csv_path) < 60):
			cattlelog.debug("The hash folder has been modified in the last " + str(time() - os.path.getmtime(csv_path)) + " seconds, so we will wait.")
			return True
		else:
			cattlelog.debug("\nNope, no concurrency problem I can smell.\n"+str(time() - os.path.getmtime(csv_path) < 60)+"\n")
			return False

	#downloads the csv json file pair from druid, returns False if the hash folder has been modified in the last 60 seconds,
	#returns True otherwise.
	def download_pair(self, f, pair):
		csv_string = requests.get(pair[0]).text
		json_string = requests.get(pair[1]).text
		self.path = make_hash_path_druid(csv_string, self.username, self.dataset, json_string)

		# if self.check_for_concurrency(): #disabled, because it should be called somewhere else!
		# 	return False

		make_hash_folder_druid(csv_string, self.username, self.dataset, json_string)

		self.path = os.path.join(self.path, secure_filename(f))

		with open(os.path.join(app.config['UPLOAD_FOLDER'], self.path), 'w') as file_csv:
			file_csv.write(csv_string)
		with open(os.path.join(app.config['UPLOAD_FOLDER'], self.path + '-metadata.json'), 'w') as file_json:
			file_json.write(json_string)

		cattlelog.debug("File {} uploaded successfully".format(os.path.join(app.config['UPLOAD_FOLDER'], self.path)))
		cattlelog.debug("File {} uploaded successfully".format(os.path.join(app.config['UPLOAD_FOLDER'], self.path + '-metadata.json')))
		return True

	def build_graph(self):
		COW(mode='convert', files=[os.path.join(app.config['UPLOAD_FOLDER'], self.path)])
		cattlelog.debug("Convert finished")
		try:
			with open(os.path.join(app.config['UPLOAD_FOLDER'], self.path + '.nq')) as nquads_file:
				g = ConjunctiveGraph()
				g.parse(nquads_file, format='nquads')
			return g
		except IOError:
			raise IOError("COW could not generate any RDF output. Please check the syntax of your CSV and JSON files and try again.")

	def upload_graph(self, graph):
		# Compress result
		out = os.path.join(app.config['UPLOAD_FOLDER'], self.path + '.nq.gz')
		with gzip.open(out, mode="w") as gzip_file:
			gzip_file.write(graph.serialize(format='application/n-quads'))

		cattlelog.debug("user: {} dataset: {} file: {}".format(self.username, self.dataset, out))

		cattlelog.debug(AUTH_TOKEN)

		# using triply's uploadFiles client
		subprocess.Popen(args=["./uploadScripts/node_modules/.bin/uploadFiles", "-t", AUTH_TOKEN, "-d", self.dataset, "-a", self.username, "-u", "https://api.druid.datalegend.net",  out])
		cattlelog.debug("Upload to Druid started..")

	def handle_pairs(self, candidates):
		# if .csv and .json pairs are present int the assets, downloads them, converts them, uploads results
		cattlelog.debug('Downloading and converting: {}'.format(candidates.keys()))
		for f in candidates.keys():

			# Download
			if not self.download_pair(f, candidates[f]):
				continue

			# Convert
			graph = self.build_graph()

			# Write StringIO to file
			# with open ('/tmp/converted.nq.gz', 'w') as fd:    #
			#     out.seek(0)                                   # commented because it error'ed here
			#     shutil.copyfileobj(out, fd)                   #

			# resp = make_response(out.getvalue())
			# resp.headers['Content-Type'] = 'application/gzip'
			# resp.headers['Content-Disposition'] = 'attachment; filename=' + f + '.nq.gz'

			# Upload
			self.upload_graph(graph)

	def download_single(self, f, candidate):
		# Downloads the csv
		csv_string = requests.get(candidate).text
		self.path = os.path.join(make_hash_folder_druid(csv_string, self.username, self.dataset), secure_filename(f))

		with open(os.path.join(app.config['UPLOAD_FOLDER'], self.path), 'w') as file_csv:
			file_csv.write(csv_string)

		cattlelog.debug("File {} uploaded successfully".format(os.path.join(app.config['UPLOAD_FOLDER'], self.path)))

	def build_auto_json(self):
		# creates a json file for a csv using COW
		cattlelog.debug("Running COW build")
		COW(mode='build', files=[os.path.join(app.config['UPLOAD_FOLDER'], self.path)])
		cattlelog.debug("Build finished")

	def found_new_json(self, current_csv):
		# Check if the json counterpart to a csv has been uploaded to the assets
		cattlelog.debug("Listing remote files in Druid for user {} dataset {}".format(self.username, self.dataset))
		r = requests.get("https://api.druid.datalegend.net/datasets/{}/{}/assets".format(self.username, self.dataset))
		files = json.loads(r.text)
		url_dict = {}
		for f in files: url_dict[f['assetName']] = f['url']
		if (current_csv + '-metadata.json') in url_dict.keys():
			return True
		else:
			return False

	# if .csv without json counterpart are present in the assets, downloads the csv, creates json, converts them, uploads results
	# during the process checks will determine whether the json counterpart still isn't uploaded, if it is found it will stop
	def handle_singles(self, candidates):
		cattlelog.debug("Waiting for possible json-files.")
		sleep(10) #wait for possible json files to be uploaded.
		cattlelog.debug("Starting with the single csv-files.")
		for f in candidates.keys():
			cattlelog.debug("handling single: " + f)
			self.download_single(f, candidates[f])
			if self.found_new_json(f):
				cattlelog.debug("Stopped because a new json was found.")
				continue
			self.build_auto_json()
			if self.found_new_json(f):
				cattlelog.debug("Stopped because a new json was found.")
				continue
			graph = self.build_graph()
			if self.found_new_json(f):
				cattlelog.debug("Stopped because a new json was found.")
				continue
			self.upload_graph(graph)


@app.route('/druid/<username>/<dataset>', methods=['POST'])
def druid(username, dataset):
	# Retrieves a list of Druid files in a dataset; if .csv and .json present, downloads them, converts them, uploads results
	# if only a csv is present a json will be created for it.
	cattlelog.debug("Starting Druid-based conversion")

	cattlelog.debug("<<<<information from the webhook:")
	cattlelog.debug(request.json)
	cattlelog.debug("end of imformation from the webhook.>>>>")

	# upload_cleanup()
	resp = make_response()

	d2c = druid2cattle(username, dataset)

	candidates, singles = d2c.get_candidates()

	if len(candidates.keys()) > 0:
		d2c.handle_pairs(candidates)
	if len(singles.keys()) > 0:
		d2c.handle_singles(singles)

	return resp, 200


# Error handlers

@app.errorhandler(404)
def pageNotFound(error):
	return render_template('error.html', error_message="Page not found")

@app.errorhandler(500)
def pageNotFound(error):
	return render_template('error.html', error_message=error.message)

if __name__ == '__main__':
	app.run(port=8088, debug=False)

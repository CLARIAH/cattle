#!/usr/bin/env python

# cattle: A Web service for COW

# from flask import Flask, request, render_template, make_response, redirect, jsonify
from werkzeug.utils import secure_filename
import logging
import requests
import os
import subprocess
import json
from rdflib import ConjunctiveGraph
from cow_csvw.csvw_tool import COW
# import StringIO
import gzip
import shutil
import traceback
from hashlib import md5
from time import sleep, time
# from updateWebhooks import update_webhooks
from mail_templates import send_new_graph_message

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

def make_hash_folder(path, csv_file, json_file=None):
	hash_folder_name = create_hash(csv_file, json_file)
	new_path = os.path.join(path, "web_interface", hash_folder_name)
	try:
		os.makedirs(new_path)
	except:
		# self.logger.debug("This folder already exists, this might result in concurrency problems.")
		pass
	return new_path

def make_hash_folder_druid(csv_string, username, dataset, path, json_string=""):
	hash_folder_name = create_hash(csv_string, json_string, False)
	try:
		os.makedirs(os.path.join(path, username, dataset, hash_folder_name))
	except:
		# self.logger.debug("This folder already exists, this might result in concurrency problems.")
		pass
	return os.path.join(username, dataset, hash_folder_name)

def make_hash_path_druid(csv_string, username, dataset, json_string=""):
	hash_folder_name = create_hash(csv_string, json_string, False)
	return os.path.join(username, dataset, hash_folder_name)


### Druid interface
class druid2cattle:
	def __init__(self, username, dataset, logger, upload_folder, requests, auth_token):
		self.username = username
		self.dataset = dataset
		self.logger = logger
		self.upload_folder = upload_folder
		self.requests = requests
		self.token = auth_token

		#changes every time
		self.path = ""

	#retrieves all the .csv-files and .csv- .json-file pairs from a specific druid dataset.
	def get_candidates(self):
		self.logger.debug("Listing remote files in Druid for user {} dataset {}".format(self.username, self.dataset))
		r = self.requests.get("https://api.druid.datalegend.net/datasets/{}/{}/assets".format(self.username, self.dataset))
		self.logger.debug("Remote files are: {}".format(r.json()))
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
		csv_path = os.path.join(self.upload_folder, self.path)
		if os.path.exists(csv_path):
			if (time() - os.path.getmtime(csv_path) < 60):
				self.logger.debug("The hash folder has been modified in the last " + str(time() - os.path.getmtime(csv_path)) + " seconds, so we will wait.")
				return True
			else:
				self.logger.debug("\nNope, no concurrency problem I can smell.\n"+str(time() - os.path.getmtime(csv_path) < 60)+"\n")
				return False
		else:
			self.logger.debug("\nNope, no folder exists, so no problem.")
			return False

	#downloads the csv json file pair from druid, returns False if the hash folder has been modified in the last 60 seconds,
	#returns True otherwise.
	def download_pair(self, f, pair):
		csv_string = self.requests.get(pair[0]).text
		json_string = self.requests.get(pair[1]).text
		self.path = make_hash_path_druid(csv_string, self.username, self.dataset, json_string)

		if self.check_for_concurrency(): #disabled, because it should be called somewhere else!
			return False

		make_hash_folder_druid(csv_string, self.username, self.dataset, self.upload_folder, json_string)

		self.path = os.path.join(self.path, secure_filename(f))

		with open(os.path.join(self.upload_folder, self.path), 'w') as file_csv:
			file_csv.write(csv_string)
		with open(os.path.join(self.upload_folder, self.path + '-metadata.json'), 'w') as file_json:
			file_json.write(json_string)

		self.logger.debug("File {} uploaded successfully".format(os.path.join(self.upload_folder, self.path)))
		self.logger.debug("File {} uploaded successfully".format(os.path.join(self.upload_folder, self.path + '-metadata.json')))
		return True

	def build_graph(self):
		COW(mode='convert', files=[os.path.join(self.upload_folder, self.path)])
		self.logger.debug("Convert finished")
		try:
			with open(os.path.join(self.upload_folder, self.path + '.nq')) as nquads_file:
				g = ConjunctiveGraph()
				g.parse(nquads_file, format='nquads')
			return g
		except IOError:
			raise IOError("COW could not generate any RDF output. Please check the syntax of your CSV and JSON files and try again.")

	def upload_graph(self, graph):
		# Compress result
		out = os.path.join(self.upload_folder, self.path + '.nq.gz')
		with gzip.open(out, mode="w") as gzip_file:
			gzip_file.write(graph.serialize(format='application/n-quads'))

		self.logger.debug("user: {} dataset: {} file: {}".format(self.username, self.dataset, out))

		# using triply's uploadFiles client
		subprocess.Popen(args=["./uploadScripts/node_modules/.bin/uploadFiles", "-t", self.token, "-d", self.dataset, "-a", self.username, "-u", "https://api.druid.datalegend.net",  out])
		self.logger.debug("Upload to Druid started..")

	def remove_files(self):
		csv_path = os.path.join(self.upload_folder, self.path)
		json_path = os.path.join(self.upload_folder, self.path + '-metadata.json')

		self.logger.debug('Removing the csv file and json file...')

		os.remove(csv_path)
		os.remove(json_path)

		self.logger.debug('Finished removing the csv file and json file.')

	def handle_pairs(self, candidates):
		# if .csv and .json pairs are present int the assets, downloads them, converts them, uploads results
		self.logger.debug('Downloading and converting: {}'.format(candidates.keys()))
		successes = []
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
			self.remove_files()
			successes.append(f)
		return successes

	def download_single(self, f, candidate):
		# Downloads the csv
		csv_string = self.requests.get(candidate).text
		self.path = os.path.join(make_hash_folder_druid(csv_string, self.username, self.dataset, self.upload_folder), secure_filename(f))

		with open(os.path.join(self.upload_folder, self.path), 'w') as file_csv:
			file_csv.write(csv_string)

		self.logger.debug("File {} uploaded successfully".format(os.path.join(self.upload_folder, self.path)))

	def build_auto_json(self):
		# creates a json file for a csv using COW
		self.logger.debug("Running COW build")
		COW(mode='build', files=[os.path.join(self.upload_folder, self.path)])
		self.logger.debug("Build finished")

	def found_new_json(self, current_csv):
		# Check if the json counterpart to a csv has been uploaded to the assets
		self.logger.debug("Listing remote files in Druid for user {} dataset {}".format(self.username, self.dataset))
		r = self.requests.get("https://api.druid.datalegend.net/datasets/{}/{}/assets".format(self.username, self.dataset))
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
		self.logger.debug("Waiting for possible json-files.")
		# sleep(10) #wait for possible json files to be uploaded. #waiting here is too late
		self.logger.debug("Starting with the single csv-files.")
		successes = []
		for f in candidates.keys():
			self.logger.debug("handling single: " + f)
			self.download_single(f, candidates[f])
			if self.found_new_json(f):
				self.logger.debug("Stopped because a new json was found.")
				continue
			self.build_auto_json()
			if self.found_new_json(f):
				self.logger.debug("Stopped because a new json was found.")
				continue
			graph = self.build_graph()
			if self.found_new_json(f):
				self.logger.debug("Stopped because a new json was found.")
				continue
			self.upload_graph(graph)
			successes.append(f)
		return successes

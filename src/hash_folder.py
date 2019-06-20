#!/usr/bin/env python

# from flask import Flask, request, render_template, make_response, redirect, jsonify
import os
import json
from hashlib import md5

#create hash to use as folder to save the files
def create_hash(csv_file, json_file, read_files=True, logger=None):
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
		n_intervals = 5.0 #if some illegal character disrupts the creation of the hash, a subset of the file is tried
		i = n_intervals
		while(i > 0):
			try:
				# print("creating a hash with {}% of the files..".format(100*(i/n_intervals)))
				m.update(csv_file[:int(round(len(csv_file)*(i/n_intervals)))]+json_file[:int(round(len(json_file)*(i/n_intervals)))])
				# print("succesfully created a hash with {}% of the files.".format(100*(i/n_intervals)))
				break
			except:
				i -= 1
		if logger != None:
			logger.debug("succesfully created a hash with {}% of the files.".format(100*(i/n_intervals)))
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
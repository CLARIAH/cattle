#!/usr/bin/env python

import os
import json
from hashlib import md5

N_INTERVALS = 5.0  # if some illegal character disrupts the creation of the hash, a subset of the file is tried

# creates a hash based on the contents of the uploaded files 
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
		i = N_INTERVALS
		while(i > 0):
			try:
				# print("creating a hash with {}% of the files..".format(100*(i/N_INTERVALS)))
				m.update(csv_file[:int(round(len(csv_file)*(i/N_INTERVALS)))]+json_file[:int(round(len(json_file)*(i/N_INTERVALS)))])
				# print("succesfully created a hash with {}% of the files.".format(100*(i/N_INTERVALS)))
				break
			except:
				i -= 1
		if logger != None:
			logger.debug("succesfully created a hash with {}% of the files.".format(100*(i/N_INTERVALS)))
	return m.hexdigest()

# creates a new folder with create_hash()
def make_hash_folder(path, csv_file, json_file=None):
	hash_folder_name = create_hash(csv_file, json_file)
	new_path = os.path.join(path, "web_interface", hash_folder_name)
	try:
		os.makedirs(new_path)
	except:
		# print("This folder already exists, this might result in concurrency problems.")
		pass
	return new_path
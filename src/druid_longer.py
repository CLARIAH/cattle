import os
from cow_csvw.csvw_tool import COW
from rdflib import ConjunctiveGraph
import gzip
import argparse
import subprocess

from mail_templates import send_new_graph_message

MAILGUN_AUTH_TOKEN = "yyy"

def build_graph(path):
	COW(mode='convert', files=[path])
	print("Convert finished")
	try:
		with open(path + '.nq') as nquads_file:
			g = ConjunctiveGraph()
			g.parse(nquads_file, format='nquads')
		return g
	except IOError:
		raise IOError("COW could not generate any RDF output. Please check the syntax of your CSV and JSON files and try again.")

def upload_graph(path, graph, token, dataset, username):
	# Compress result
	out = path + '.nq.gz'
	with gzip.open(out, mode="w") as gzip_file:
		gzip_file.write(graph.serialize(format='application/n-quads'))

	# using triply's uploadFiles client
	subprocess.Popen(args=["./uploadScripts/node_modules/.bin/uploadFiles", "-t", token, "-d", dataset, "-a", username, "-u", "https://api.druid.datalegend.net", "-p", out])
	print("Upload to Druid started..")

def remove_files(path):
	csv_path = path
	json_path = path + '-metadata.json'
	print('Removing the csv file and json file...')

	os.remove(csv_path)
	os.remove(json_path)
	print('Finished removing the csv file and json file.')

def send_email(email_address, account_name, path):
	try:
		if email_address != "" and account_name != "":
			send_new_graph_message(email_address, account_name, os.path.basename(path), MAILGUN_AUTH_TOKEN)
	except:
		pass

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="")
	parser.add_argument('-path', type=str, default='', help='join of upload_folder and path, location of the files')
	parser.add_argument('-token', type=str, default='', help='druid token')
	parser.add_argument('-dataset', type=str, default='', help='')
	parser.add_argument('-username', type=str, default='', help='')
	parser.add_argument('-email_address', type=str, default='', help='')
	parser.add_argument('-account_name', type=str, default='', help='')

	args = parser.parse_args()

	graph = build_graph(args.path)
	upload_graph(args.path, graph, args.token, args.dataset, args.username)
	remove_files(args.path)

	send_email(args.email_address, args.account_name, args.path)
import os
from cow_csvw.csvw_tool import COW
from rdflib import ConjunctiveGraph
import gzip	

def build_graph(path):
	COW(mode='convert', files=[path])
	# self.logger.debug("Convert finished")
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

	# self.logger.debug("user: {} dataset: {} file: {}".format(self.username, self.dataset, out))

	# using triply's uploadFiles client
	subprocess.Popen(args=["./uploadScripts/node_modules/.bin/uploadFiles", "-t", token, "-d", dataset, "-a", username, "-u", "https://api.druid.datalegend.net", "-p", out])
	# self.logger.debug("Upload to Druid started..")

def remove_files(path):
	csv_path = path
	json_path = path + '-metadata.json'

	# self.logger.debug('Removing the csv file and json file...')

	os.remove(csv_path)
	os.remove(json_path)

	# self.logger.debug('Finished removing the csv file and json file.')

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="")
	parser.add_argument('-path', type=str, default='', help='join of upload_folder and path, location of the files')
	parser.add_argument('-token', type=str, default='', help='')
	parser.add_argument('-dataset', type=str, default='', help='')
	parser.add_argument('-username', type=str, default='', help='')

	args = parser.parse_args()

	graph = build_graph(args.path)

	# upload_graph(args.path, graph)

	# remove_files(args.path)


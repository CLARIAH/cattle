import os
from cow_csvw.csvw_tool import COW
# from rdflib import ConjunctiveGraph
import argparse
# import subprocess
import threading

# import logging
# # Logging
# LOG_FORMAT = '%(asctime)-15s [%(levelname)s] (%(module)s.%(funcName)s) %(message)s'
# app.debug_log_format = LOG_FORMAT
# logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
# cattlelog = logging.getLogger(__name__)

def build_graph(path):
    COW(mode='convert', files=[path])
    print("Convert finished")
    # try:
    #   with open(path + '.nq') as nquads_file:
    #       g = ConjunctiveGraph()
    #       g.parse(nquads_file, format='nquads')
    #   return g
    # except IOError:
    #   raise IOError("COW could not generate any RDF output. Please check the syntax of your CSV and JSON files and try again.")

def remove_files(path):
    csv_path = path
    json_path = path + '-metadata.json'
    print('Removing the csv file and json file...')

    os.remove(csv_path)
    os.remove(json_path)
    print('Finished removing the csv file and json file.')

def convert_remove(path):
    build_graph(path)
    remove_files(path)

def create_thread(path, logger=None):
    if logger != None:
        logger.debug("PATH: {}".format(path))
    convert_thread = threading.Thread(target=convert_remove, args=[path])
    convert_thread.start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('-path', type=str, default='', help='join of upload_folder and path, location of the files')

    args = parser.parse_args()

    # build_graph(args.path)
    # remove_files(args.path)

    create_thread(args.path)
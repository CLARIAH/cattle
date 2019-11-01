#!/usr/bin/env python

# In this file the threaded part of Cattle is performed, in order
# to keep the long process of building the graph seperate from the
# web interface part of Cattle.

import os
from cow_csvw.csvw_tool import COW
import argparse
import threading


def build_graph(path):
    COW(mode='convert', files=[path])
    print("Convert finished")

#remove the files required for the creation of the linked data
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
    parser.add_argument('-path', type=str, default='', help='Full path to the location of the files.')
    args = parser.parse_args()

    create_thread(args.path)
#!/usr/bin/env python

from flask import Flask
from subprocess import call
import os
app = Flask(__name__)

@app.route("/", methods=['POST'])
def update():
    print "Starting image update"
    call(['docker', 'pull', 'clariah/cattle:dev'])
    call(['docker', 'pull', 'clariah/cattle:latest'])
    print "Restarting images"
    call(['docker-compose', '-f', '/home/clariah-sdh/src/cattle-dev/docker-compose.yml', 'restart'])
    call(['docker-compose', '-f', '/home/clariah-sdh/src/cattle/docker-compose.yml', 'restart'])
    print "All done; exiting..."

    return 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8004, debug=True)

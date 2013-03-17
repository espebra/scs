#!/usr/bin/python
from flup.server.fcgi import WSGIServer
from director import app

if __name__ == '__main__':
    WSGIServer(app).run()

#!/usr/bin/python

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from storage import app

http_server = HTTPServer(WSGIContainer(app))
http_server.listen(1029)
IOLoop.instance().start()

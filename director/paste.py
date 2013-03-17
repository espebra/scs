#!/usr/bin/python

import paste
from paste import httpserver
from director import app

paste.httpserver.serve(app, host=app.config['HOST'], port=app.config['PORT'])


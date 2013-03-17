#!/usr/bin/python

import paste
from paste import httpserver
from director import app

paste.httpserver.serve(app, host="0.0.0.0", port=1028)


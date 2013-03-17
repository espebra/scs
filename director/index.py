#!/usr/bin/python
from wsgiref.handlers import CGIHandler
from director import app

CGIHandler().run(app)

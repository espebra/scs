#!/usr/bin/python
from wsgiref.handlers import CGIHandler
from storage import app

CGIHandler().run(app)

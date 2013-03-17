#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import shutil
import tempfile
import hashlib

import flask
import werkzeug

app = flask.Flask(__name__)
app.config.from_pyfile('storage.cfg')
app.config.from_pyfile('local.cfg', silent=True)

def secure_name(value):
    return werkzeug.utils.secure_filename(value)

def file_content_checksum(path):
    m = hashlib.sha256()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(128*m.block_size), b''):
            m.update(chunk)

    f.close()
    return m.hexdigest()

# Calculate the time elapsed to process each request
@app.before_request
def before_request():
    app.config['FROM'] = time.time()
    try:
        flask.request.args['info']
    except:
        app.config['INFO'] = False
    else:
        app.config['INFO'] = True

@app.after_request
def after_request(resp):
    app.config['TO'] = time.time()
    elapsed = app.config['TO'] - app.config['FROM']
    # TODO: May be logged and graphed (for SLA purposes)
    resp.headers.add('elapsed', elapsed)
    return resp

@app.route("/<account>/<bucket>/<obj>", methods = ["GET", "PUT"])
def object(account, bucket, obj):

    account = secure_name(account)
    bucket  = secure_name(bucket)
    obj     = secure_name(obj)

    host = flask.request.headers['HOST']
    dir_path = '%s/%s/%s/%s' % ( \
        app.config['STORAGE_DIRECTORY'], host, account, bucket)
    file_path = '%s/%s' % (dir_path, obj)

    if flask.request.method == 'PUT':
        # The temporary destination (while the upload is still in progress)
        try:
            temp = tempfile.NamedTemporaryFile(dir = app.config['TEMP_DIRECTORY'])
        except:
            # Unable to create temp file
            app.logger.error("Unable to create named temprary file %s" % (temp.name))
            flask.abort(500)

        # Upload to a temp file
        try:
            temp.write(flask.request.data)
            temp.seek(0)
    
        except:
            app.logger.error("Unable to write to named temprary file %s" % (temp.name))
            flask.abort(500)
    
        else:
            app.logger.debug("File uploaded successfully to %s" % (temp.name))

        # Ensure directory structure
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path)
            except:
                app.logger.error("Unable to create the directory structure" % (dir_path))
                flask.abort(500)

        # Copy the temp file to the target destination path
        try:
            shutil.copyfile(temp.name,file_path)
            temp.close()
        except:
            app.logger.error("Unable to copy temp file (%s) to target path (%s)" % (temp.name,file_path))
            flask.abort(500)

        checksum = file_content_checksum(file_path)
        out='OK %s\n' % checksum

        response = flask.make_response(out)
        response.headers['status'] = 200
        response.headers['content-type'] = 'text/plain'
        return response

    elif flask.request.method == 'GET':

        try:
            st = os.stat(file_path)
        except:
            code = 404
        else:
            code = 200

        out = ''
        if app.config['INFO']:
            if code == 200:
                out += 'host = %s\n' % host
                out += 'account = %s\n' % account
                out += 'bucket = %s\n' % bucket
                out += 'object = %s\n' % obj
                out += 'path = %s\n' % file_path
                out += 'mtime = %s\n' % st.st_mtime
                out += 'size = %s\n' % st.st_size

            response = flask.make_response(out)
            response.headers['status'] = code
            response.headers['content-type'] = 'text/plain'
            return response
        else:
            if code == 200:
                response = flask.make_response( \
                    flask.send_file(file_path, as_attachment = False))
                #response.headers['cache-control'] = 'max-age=86400, must-revalidate'
                return response
            else:
                response = flask.make_response(out)
                response.headers['status'] = code
                response.headers['content-type'] = 'text/plain'
                return response


if __name__ == '__main__':
    app.run(host=app.config['HOST'], \
            port=app.config['PORT'], \
            debug=app.config['DEBUG'])


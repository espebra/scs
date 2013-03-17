#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import yaml
import hashlib
import urllib2
import random
import time
import threading
import Queue

import flask
import werkzeug

import ConsistentHash

app = flask.Flask(__name__)
app.config.from_pyfile('director.cfg')
app.config.from_pyfile('/etc/scs/director/local.cfg', silent=True)

def secure_name(value):
    return werkzeug.utils.secure_filename(value)

def generate_hash(account, bucket, obj):
    # TODO: Find a cheaper method here
    m = hashlib.sha1()

    # If the configuration contains a salt, use it to make the hash somewhat
    # more difficult to guess
    if 'SALT' in app.config:
        m.update(app.config['SALT'] + account + bucket + obj)
    else:
        m.update(account + bucket + obj)

    h = m.hexdigest()
    return h

def get_regions(h, resources, region_count):
    regions = sorted(resources)
    return get_consistent_hash_values(h, regions, region_count)

def get_nodes(h, resources, replica_count, regions = []):
    selected_nodes = []

    # Select the regions that are provided to the function
    for region in regions:
        nodes_in_region = resources[region]
        selected_nodes += get_consistent_hash_values(h,nodes_in_region,replica_count)

    return selected_nodes

# http://entitycrisis.blogspot.no/2010/05/consistent-hashing-in-python-redux.html
def get_consistent_hash_values(h,nodes,replicas = 2):
    ret = []
    ring = ConsistentHash.ConsistentHash()

    # Create the ring
    for node in nodes:
        ring.add(node)

    for i in range(replicas):
        node = ring.hash(h)
        ret.append(node)
        ring.remove(node)

    return ret


def query_storage_nodes(nodes,function,a):
    ret = {}
    q = Queue.Queue()
    for node in nodes:
        threading.Thread(target=function, args=(node, q, a)).start()
        ret[node] = q.get()
    return ret

def get_node_status(node, q = False, a = None):

    url = "http://%s/ping" % (node)
    code = 404
    try:
        conn = urllib2.urlopen(url, timeout = 1)
        code = conn.getcode()
        conn.close()
    except urllib2.HTTPError, e:
        code = e.getcode()

    q.put(code)

def get_object_status_on_node(node, q = False, a = None):

    account = a[0]
    bucket  = a[1]
    obj     = a[2]
    url = "http://%s/%s/%s/%s?info" % (node, account, bucket, obj)
    code = 404
    try:
        conn = urllib2.urlopen(url, timeout = 1)
        code = conn.getcode()
        conn.close()
    except urllib2.HTTPError, e:
        code = e.getcode()

    ret = {}
    ret['code'] = code
    q.put(ret)

def select_node(object_status):
    ok = []
    for node in object_status:
        if object_status[node]['code'] == 200:
            ok.append(node)

    if len(ok) > 0:
        return random.choice(ok)
    else:
        return None

# Calculate the time elapsed to process each request
@app.before_request
def before_request():
    app.config['FROM'] = time.time()
    try:
        flask.request.args['debug']
    except:
        app.config['DEBUG'] = False
    else:
        app.config['DEBUG'] = True

    try:
        flask.request.args['nodes']
    except:
        app.config['NODES'] = False
    else:
        app.config['NODES'] = True

    try:
        flask.request.args['info']
    except:
        app.config['INFO'] = False
    else:
        app.config['INFO'] = True

    # Read configuration
    f = app.config['RESOURCES_FILE']
    try:
        fp=open(f)
        cfg=yaml.load(fp)
        fp.close()

    except:
        flask.abort(500)

    else:
        app.config['RESOURCES'] = cfg

@app.after_request
def after_request(resp):
    app.config['TO'] = time.time()
    elapsed = app.config['TO'] - app.config['FROM']
    # TODO: May be logged and graphed (for SLA purposes)
    resp.headers.add('elapsed', elapsed)
    return resp

@app.route("/<account>/<bucket>")
#def bucket(account, bucket):
#
#    account = secure_name(account)
#    bucket  = secure_name(bucket)
#
#    if flask.request.method == 'GET':
#        objects = get_objects_in_bucket(account, bucket)
#
#        out = 'foo'
#
#        response = flask.make_response(out)
#        response.headers['status'] = '200'
#        response.headers['content-type'] = 'text/plain'
#        #response.headers['cache-control'] = 'max-age=60, must-revalidate'
#        return response

@app.route("/<account>/<bucket>/<obj>", methods = ["GET", "PUT"])
def object(account, bucket, obj):

    account = secure_name(account)
    bucket  = secure_name(bucket)
    obj     = secure_name(obj)

    h = generate_hash(account, bucket, obj)

    resources = app.config['RESOURCES']
    replica_count = int(app.config['NUMBER_OF_REPLICAS_PER_REGION'])
    region_count = int(app.config['NUMBER_OF_REGIONS_TO_STORE_EACH_OBJECT'])

    # If the regions are not set for a bucket, generate according to the
    # region_count variable set in the global configuration.
    regions = get_regions(h, resources, region_count)

    # Get the nodes for this hash
    nodes = get_nodes(h, resources, replica_count, regions)

    url = None
    if len(nodes) > 0:
        url = "http://%s/%s/%s/%s" % (random.choice(nodes), account, bucket, obj)

    if flask.request.method == 'PUT':

        # We should check the status of the nodes here, so that the client is
        # redirected to an available node.
        #nodes_status = query_storage_nodes(nodes,get_node_status,None)

        #for 
        #    selected_node = get_available_node(nodes)
        #    if selected_node:
        #        url = "http://%s/%s/%s/%s" % (selected_node, account, bucket, obj)

        if url:
            # 307 Temporary Redirect
            # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
            response = flask.redirect(url, code = 307)
            # http://www.w3.org/Protocols/rfc2616/rfc2616-sec8.html#sec8.2.3
            #response.headers['expect'] = '100-continue'
            return response

    elif flask.request.method == 'GET':
    
        # Find out what the http return codes are on this specific object, and use
        # it to redirect the client to the correct storage node.
        object_status = query_storage_nodes( \
            nodes,get_object_status_on_node,(account,bucket,obj))

        if len(nodes) > 0:
            selected_node = select_node(object_status)
            if selected_node:
                url = "http://%s/%s/%s/%s" % (selected_node, account, bucket, obj)

        if app.config['NODES']:
            out = ''
            for node in nodes:
                out += '%s [%s]\n' % (node, object_status[node]['code'])

            response = flask.make_response(out)

            if len(nodes) > 0:
                response.headers['status'] = '200'
            else:
                response.headers['status'] = '404'

            response.headers['content-type'] = 'text/plain'
            return response

        elif app.config['INFO']:
            out = ''

            out += 'account = %s\n' % account
            out += 'bucket = %s\n' % bucket
            out += 'object = %s\n' % obj

            out += 'replicas_per_region = %d\n' % int(app.config['NUMBER_OF_REPLICAS_PER_REGION'])
            out += 'regions = %d\n' % int(app.config['NUMBER_OF_REGIONS_TO_STORE_EACH_OBJECT'])

            out += 'hash = %s\n' % h
            for node in nodes:
                out += 'node = %s [%s]\n' % (node, object_status[node]['code'])

            for region in regions:
                out += 'region = %s\n' % region
            if url:
                out += 'url = %s\n' % url

            response = flask.make_response(out)
            response.headers['status'] = '200'
            response.headers['content-type'] = 'text/plain'
            #response.headers['cache-control'] = 'max-age=60, must-revalidate'
            return response
        else:
            if url:
                if not app.config['DEBUG']:
                    return flask.redirect(url, code = 302)

            else:
                response = flask.make_response('not found')
                response.headers['status'] = '404'
                response.headers['content-type'] = 'text/plain'
                return response


if __name__ == '__main__':
    app.run(host=app.config['HOST'], \
            port=app.config['PORT'], \
            debug=app.config['DEBUG'])


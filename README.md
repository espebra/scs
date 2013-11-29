# Simple Cloud Storage

This is a proof of concept minimalistic distributed object store implemented in
Lua, based on the OpenResty web application server. 


## Features

* Fast.
* Scalable.
* Stateless.
* Non-SPOF.
* Active/active multi location.
* Eventually consistent.


## Design principles

* Keep it simple.
* Requests can be sent to any of the hosts. The client will be redirected to one of the replica hosts for that specific request. This will provide high availability when used with anycast and/or IP failover.
* Automatic rebalancing if hosts are down is not necessary. It is sufficient to rebalance when hosts are added and/or removed from the configuration file.
* Stateless is robust.
* It must be easy to export the objects from scs as regular files allowing them to be imported into another storage solutions.


## Installation

To get a development/testing environment up and running:

    # git clone https://github.com/espebra/scs
    # cd scs
    # vagrant up

A cluster with virtual machines running scs on CentOS 6 will be configured and started. The three hosts in the development environment will get the names and IPv4 addresses:

* scs1.local -> 10.0.0.2
* scs2.local -> 10.0.0.3
* scs3.local -> 10.0.0.4

Now that the build process is complete, try out the examples below.


## Example usage

The commands below can be copy/pasted directly to write, read and delete an object from your development environment. The requests are being directed to 10.0.0.4, but they can be sent to any of the hosts.


### Write object

The following will upload the content of the file *sourcefile* to the bucket *somebucket* with the file name *targetfile*. Targetfile may contain the character /, which will make it look like a directory structure. The request can be sent to all of the hosts in the cluster, and the result will be the same:

    # cd /tmp/
    # echo "foobar" > sourcefile
    # md5=$(md5sum sourcefile | awk '{print $1}')
    # curl -s -L -H "expect: 100-continue" -H "x-md5: $md5" --data-binary "@sourcefile" "http://10.0.0.4/targetfile?bucket=somebucket" | python -mjson.tool

To make it a bit easier to handle different buckets in the development environment, the bucket can be specified as a parameter ''bucket'' in the URL as shown above. In production environments, the bucket is being read from the server name used in the request. The two URLs below are equally handled given that somebucket.scs.example.com points to 10.0.0.4:

    # http://10.0.0.4/targetfile?bucket=somebucket
    # http://somebucket.scs.example.com/targetfile

When the upload is complete, an entry is made in */srv/files/queue/* marking this object as changed. A replicator daemon monitors this directory and will replicate the objects found to the other replica hosts these objects should be replicated to according to their hash.

The file *targetfile* is stored on the number of replica hosts and sites specified in */etc/scs/common.conf*. 


### Read object

The following will download *targetfile* from the bucket *somebucket*. The request can be sent to all of the hosts in the cluster, and the result will be the same:

    # curl -s -L "http://10.0.0.4/targetfile?bucket=somebucket"

Extra information about the object:

    # curl -s -L "http://10.0.0.4/targetfile?bucket=somebucket&x-meta" | python -mjson.tool

What happens is that the host that handles the request will lookup which hosts actually have *targetfile* on their local file systems (replica hosts), and redirect (302) your client to one of these for a direct download.


### Delete object

The following will delete *targetfile* from the bucket *somebucket*. The request can be sent to all of the hosts in the cluster, and the result will be the same:

    # curl -s -L -X "DELETE" "http://10.0.0.4/targetfile?bucket=somebucket" | python -mjson.tool

Confirm that the object has been removed by trying to read it. The following should respond with a HTTP 404 Not Found:

    # curl -i -s -L "http://10.0.0.4/targetfile?bucket=somebucket"


## Troubleshooting

* Ensure that you connect to scs using a valid hostname, or that you specify the bucket as a parameter if connecting to scs using an IP address.
* Ensure that xinetd or rsyncd is running, listening on port 873/tcp.
* Ensure that /srv/files and /var/log/scs is writable by the user running scs.
* /var/log/messages and /var/log/scs/error.log will provide information which is useful for debugging.


## Roadmap

* List available versions of given objects.
* Authentication.
* Lazy replication daemon to scan through all objects (not just the queue) and perform replication.
* Reaper daemon to quarantine files with invalid md5 checksums.
* API compability with some of the most used features in S3.


## Status

Not production ready. The API may change.


## Thanks

* Tor Hveem


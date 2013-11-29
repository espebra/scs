# Simple Cloud Storage

This is a proof of concept minimalistic distributed object store implemented in
Lua, based on the OpenResty web application server. 

## Features

* Fast
* Highly scalable
* Stateless
* Non-SPOF
* Active/active multi location
* Eventual consistent

## Design principles

* Keep it simple.
* Requests can be sent to any of the hosts. The client will be redirected to one of the replica hosts for that specific request. This will provide high availability when used with anycast and/or IP failover.
* Automatic rebalancing if hosts are down is not necessary. It is sufficient to rebalance when hosts are added and/or removed from the configuration file.
* Stateless is robust.

## Cloudy roadmap

* Authentication
* Cluster syncronization

 * On PUT
 * On DELETE
 * At host recovery
 * When adding and removing hosts

* API compability with some of the features in S3

## Installation

To get a development/testing environment up and running:

    # git clone https://github.com/espebra/scs
    # cd scs
    # vagrant up

## Example usage

### Upload

The following will upload the content of the file *sourcefile* to the bucket *somebucket* with the file name *targetfile*. Targetfile may contain the character /, which will make it look like a directory structure. The request can be sent to all of the hosts in the cluster, and the result will be the same:

    # md5=$(md5sum /path/to/sourcefile | awk '{print $1}')
    # curl -L -H "expect: 100-continue" -H "x-md5: $md5" --data-binary "@/path/to/sourcefile" "http://10.0.0.3/targetfile?bucket=somebucket"

To make it a bit easier to handle different buckets in the development environment, the bucket can be specified as a parameter as shown above. In production environments, this parameter may be given as the first part of the fqdn which is used:

    # md5=$(md5sum /path/to/sourcefile | awk '{print $1}')
    # curl -L -H "expect: 100-continue" -H "x-md5: $md5" --data-binary "@/path/to/sourcefile" "http://somebucket.scs.example.com/targetfile"

The filename will be base64 encoded to allow weird characters, and will be stored in the file system in the directory */srv/files/objects/somebucket/d/G/F/dGFyZ2V0ZmlsZW5hbWU=/*. *somebucket* is the bucket name, */d/G/F/* is a directory structure to allow many files within the bucket and *dGFyZ2V0ZmlsZcKg* is the base64 encoded filename *targetfile*. When the upload is complete, an entry is be made in */srv/files/queue/* marking this object as changed. A replicator daemon monitors this directory and will replicate the objects found to the other replica hosts these objects should be replicated to according to their hash.

The file *targetfile* is stored on the number of replica hosts and sites specified in */etc/scs/common.conf*. 

### Download

The following will download *targetfile* from the bucket *somebucket*. The request can be sent to all of the hosts in the cluster, and the result will be the same:

    # curl -L "http://10.0.0.3/targetfile?bucket=somebucket"

Or, using the fqdn to specify bucket:

    # curl -L "http://somebucket.scs.example.com/targetfile"

What happens is that the host that handles the request will lookup which hosts actually have *targetfile* on their local file systems (replica hosts), and redirect (302) your client to one of these - quite randomly.

### DELETE

The following will delete *targetfile* from the bucket *somebucket*. The request can be sent to all of the hosts in the cluster, and the result will be the same:

    # curl -L -X "DELETE" "http://10.0.0.3/targetfile?bucket=somebucket"

Or, using the fqdn to specify bucket:

    # curl -L -X "DELETE" "http://somebucket.scs.example.com/targetfile"

## Troubleshooting

* Ensure that you connect to scs using a valid hostname, or that you specify the bucket as a parameter if connecting to scs using an IP address.
* Ensure that xinetd or rsyncd is running, listening on port 873/tcp.
* Ensure that /srv/files and /var/log/scs is writable by the user running scs.

## Thanks

* Tor Hveem


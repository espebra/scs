# Simple Cloud Storage

This is a proof of concept minimalistic distributed object store implemented in
Lua. 

## Features

* Stateless
* Non-SPOF
* Multi location
* Eventual consistent (WIP)

## Design principles

* Requests can be sent to any of the hosts. The requests will be redirected to one of the correct ones. This is suitable for anycast.
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

Install xinetd and enable rsyncd (/etc/xinetd.d/rsync).

Download and install OpenResty (http://openresty.org/). We assume that OpenResty is installed to /usr/local/openresty/, but feel free to install it to another location.

Example:

    # wget http://openresty.org/download/ngx_openresty-1.2.8.1.tar.gz
    # tar -xvzf ngx_openresty-1.2.8.1.tar.gz
    # cd ngx_openresty-1.2.8.1
    # ./configure --with-luajit --with-pcre-jit --prefix=/usr/local/openresty
    # make
    # make install

Create the directory structure, fetch scs.

    # mkdir -p /srv/scs /srv/files /etc/scs /var/log/scs
    # cd /srv/scs
    # git clone https://github.com/espebra/scs.git

Copy the example configuration files and edit to suit your setup.

    # cp /srv/scs/conf/scs.json.example /etc/scs/scs.json
    # cp /srv/scs/conf/rsyncd.conf /etc/

Copy the example init script.

    # cp /srv/scs/conf/scs.init.example /etc/init.d/scs

You should now be good to go.

## Example usage

### Upload

The following will upload the content of the file *sourcefile* to the bucket *somebucket* with the file name *targetfile*.

    # curl -L -H 'expect: 100-continue' --data-binary "@sourcefile" http://somebucket.scs.company.com/targetfile

### Download

The following will download *targetfile* from the bucket *somebucket*.

    # GET http://somebucket.scs.company.com/targetfile

## Troubleshooting

* Ensure that you connect to scs using a valid hostname, not the IP-address of the host. The host header is being used as the name of the bucket.
* Ensure that xinetd or rsyncd is running, listening on port 873/tcp.
* Ensure that /srv/files and /var/log/scs is writable by the user running scs.

## Thanks

* Tor Hveem


Simple Cloud Storage
====================

This is a proof of concept minimalistic distributed object store implemented in
Lua. 

Features
--------
* Stateless
* Non-SPOF
* Multi location
* Eventual consistent (WIP)

Design principles
-----------------
* Requests can be sent to any of the hosts. The requests will be redirected to one of the correct ones. This is suitable for anycast.
* Automatic rebalancing if hosts are down is not necessary. It is sufficient to rebalance when hosts are added and/or removed from the configuration file.
* Stateless is robust.

Cloudy roadmap
--------------

* Authentication
* Cluster syncronization

 * On POST
 * On PUT
 * On DELETE
 * At host recovery
 * When adding and removing hosts

* API compability with some of the features in S3

Thanks
------
* Tor Hveem


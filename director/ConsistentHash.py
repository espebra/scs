#!/usr/bin/python
# http://entitycrisis.blogspot.no/2010/05/consistent-hashing-in-python-redux.html

import hashlib
import bisect


class ConsistentHash(object):
    def __init__(self, D=200):
        self.keyspace = []
        self.D = D 

    def partition(self, arg):
        h = hashlib.sha1(str(arg)).hexdigest()
        return int(h[:16], 16) 

    def add(self, hash):
        for i in xrange(self.D):
            k = self.partition("%s.%s"%(hash,i))
            bisect.insort(self.keyspace, (k, hash))

    def remove(self, hash):
        self.keyspace = [i for i in self.keyspace if i[1] != hash] 

    def __getitem__(self, i): 
        return self.keyspace[i%len(self.keyspace)][1] 

    def hash(self, key):
        p = self.partition(key)
        i = bisect.bisect_left(self.keyspace, (p,None))
        return self[i-1]


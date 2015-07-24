#coding:utf-8
#author:tanzhenhua

import memcache
import binascii
import bisect 

def newmemcache_hash(key):
    code = binascii.crc32(key.encode('ascii'))
    if code < 0:
        code = ~code
    return code


class ConstHashClient(memcache.Client):
      	
    def set_servers(self, servers):
        """Set the pool of servers used by this client.

        @param servers: an array of servers.
        Servers can be passed in two forms:
            1. Strings of the form C{"host:port"}, which implies a
            default weight of 1.
            2. Tuples of the form C{("host:port", weight)}, where
            C{weight} is an integer weight value.

        """
        #self.servers = []
        ###add by tzh start
        _virtualNodes = {}
        _max_vitual_nodes = 250
        _virtualNodeArray = range(_max_vitual_nodes)
        self.buckets = []
        _servers = []
        for s in servers:
            _host = memcache._Host(s, self.debug, dead_retry=self.dead_retry,
                              socket_timeout=self.socket_timeout,
                              flush_on_reconnect=self.flush_on_reconnect)
            address = ''
            if isinstance(s,tuple):
               	address = s[0] 
            else:
                address = s

            for i in _virtualNodeArray:
                key = '%s-node%d' % (address, i)
                node_code = newmemcache_hash(key)
                if _virtualNodes.has_key(node_code):
                    raise Exception('has same server!')
                _virtualNodes[node_code] = _host    
                self.buckets.append(node_code) 
  		self.buckets.sort() 
            _servers.append(_host) 
        self._virtualNodes = _virtualNodes
        self.servers = _servers
        ###add by tzh end 
               	
        #self._init_buckets()
	
    def _get_server(self, key):
        if isinstance(key, tuple):
            serverhash, key = key
        else:
            #serverhash = serverHashFunction(key)
            ###update by tzh
            serverhash = newmemcache_hash(key) 

        if not self.buckets:
            return None, None

        for i in range(memcache.Client._SERVER_RETRIES):
            #server = self.buckets[serverhash % len(self.buckets)]
            ### update by tzh
            bucketIndex = bisect.bisect_right(self.buckets, serverhash)  
            if bucketIndex >= len(self.buckets):
                bucketIndex = 0
            server = self._virtualNodes[self.buckets[bucketIndex]]
            if server.connect():
                print("(using server %s)" % server,)
                return server, key
            serverhash = serverHashFunction(str(serverhash) + str(i))
        return None, None

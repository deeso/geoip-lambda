import web
import json
from geoip import geolite2
from requests import Session, Request
import socket
import logging
import SocketServer
from ghettoq.simple import Connection


class GeoIPEnrich:
    MY_URL = '/geoip-enrich/'
    MY_ADDR = '0.0.0.0'
    MY_PORT = 8989
    REDIS_Q = None

    GEOIP_ENRICHER = None

    DEFAULT_FORWARD_TYPE = "tcp"
    DEFAULT_FORWARD_HOST = '127.0.0.1'
    DEFAULT_FORWARD_PORT = 6767
    DEFAULT_FORWARD_PROTO = "http"
    DEFAULT_FORWARD_URI = ""

    UPDATE_JSON_KEYS = []
    UPDATE_JSON_KEYS_PREPEND = {}

    @classmethod
    def urls(cls):
        return [(cls.MY_URL, cls)]

    @classmethod
    def set_my_info(cls, host, port, url=''):
        cls.MY_ADDR = host
        cls.MY_PORT = port
        cls.MY_URL = url

    @classmethod
    def get_server(cls):
        try:
            logging.debug("starting the geoip-lambda server listener")
            server = SocketServer.UDPServer((cls.MY_ADDR, cls.MY_PORT), cls)
            return server
        except:
            raise

    @classmethod
    def get_redis_queue_server(cls):
        try:
            logging.debug("starting the geoip-lambda redis queue listener")
            cls.REDIS_Q = Connection("redis", host=cls.MY_ADDR, database=1)
            return cls.REDIS_Q.Queue(cls.MY_URL)
        except:
            raise

    @classmethod
    def set_enrichment_keys(cls, keys_prepend_list):
        cls.UPDATE_JSON_KEYS = []
        cls.UPDATE_JSON_KEYS_PREPEND = {}
        for k, p in keys_prepend_list:
            cls.UPDATE_JSON_KEYS_PREPEND[k] = p
            cls.UPDATE_JSON_KEYS.append(k)

    @classmethod
    def set_forward(cls, host, port, uri='', proto='http'):
        cls.DEFAULT_FORWARD_HOST = host
        cls.DEFAULT_FORWARD_PORT = port
        cls.DEFAULT_FORWARD_PROTO = uri
        cls.DEFAULT_FORWARD_URI = proto

    @classmethod
    def set_url(cls, new_url):
        cls.MY_URL = new_url

    @classmethod
    def get_url(cls):
        return cls.MY_URL

    @classmethod
    def do_request(cls, uri, data, headers={}, timeout=3.0):
        s = Session()
        req = Request('POST', uri, data=data, headers=headers)
        prepped = req.prepare()

        resp = s.send(prepped,
                      stream=True,
                      verify=False,
                      proxies={},
                      cert=None,
                      timeout=timeout)
        return resp.status_code

    @classmethod
    def do_udp(cls, host, port, data):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return sock.sendto((host, port), data)

    @classmethod
    def do_redis_gq(cls, host, queuename, data):
        conn = Connection("redis", host=host, database=1)
        q = conn.Queue(queuename)
        q.put(data)
        return len(data)

    @classmethod
    def get_uri(cls, **kargs):
        return "{proto}://{host}:{port}/{uri}"

    @classmethod
    def forward_data(cls, data):
        if cls.DEFAULT_FORWARD_TYPE == 'tcp':
            kargs = {
                     'proto': cls.DEFAULT_FORWARD_PROTO,
                     'host': cls.DEFAULT_FORWARD_HOST,
                     'port': cls.DEFAULT_FORWARD_PORT,
                     'uri': cls.DEFAULT_FORWARD_URI
                     }
            uri = cls.get_uri(**kargs)
            logging.debug("Forwarding result to tcp:%s" % uri)
            return cls.do_request(uri, data, cls.headers, cls.timeout)
        elif cls.DEFAULT_FORWARD_TYPE == 'redis':
            host = cls.DEFAULT_FORWARD_HOST
            queuename = cls.DEFAULT_FORWARD_URI
            msg = json.dumps(data)
            return cls.do_redis_gq(host, queuename, msg)
        else:  # cls.DEFAULT_FORWARD_PROTO == 'udp':
            host, port = cls.DEFAULT_FORWARD_HOST, cls.DEFAULT_FORWARD_PORT
            logging.debug("Forwarding result to udp:%s:%s" % (host, port))
            msg = json.dumps(data) + '\n'
            return cls.do_udp(host, port, msg)

    def POST(self):
        logging.debug("Handling enrich POST message")
        data = json.loads(web.data())
        return self.enrich(data)

    @classmethod
    def perform_look_up(self, data):
        logging.debug("Performing look-ups")
        data_keys = [i for i in data.keys() if i in self.UPDATE_JSON_KEYS]
        for k in data_keys:
            r = None
            logging.debug("Enriching %s key" % k)
            if self.GEOIP_ENRICHER is None:
                r = geolite2.lookup(data[k])
            else:
                r = self.GEOIP_ENRICHER.lookup(data[k])

            if r is not None:
                p = self.UPDATE_JSON_KEYS_PREPEND[k]
                logging.debug("Updating %s key with %s" % (k, "_".join(p, k)))
                for k, v in r.items():
                    data["_".join(p, k)] = v
        return data

    @classmethod
    def enrich(self, data):
        data = self.perform_look_up(data)
        result = self.forward_data(data)
        logging.debug("Enriching and forward complete: result = %s" % result)
        return result

    def handle(self):
        logging.debug("Handling enrich UDP message")
        bdata = bytes.decode(self.request[0].strip())
        data = json.loads(bdata)
        return self.enrich(data)

    @classmethod
    def perform_redis_poll(cls):
        q = cls.get_redis_queue_server()
        while True:
            try:
                message = q.get()
                data = json.loads(message)
                GeoIPEnricher.enich(data)
            except:
                pass

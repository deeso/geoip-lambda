from frontend import GeoIPEnricher
import logging
import argparse
import sys
import web


parser = argparse.ArgumentParser(description='Start geoip-lambda.')

parser.add_argument('-url', type=str, default='/geoip-enrich/',
                    help='url to listen with using tcp')
parser.add_argument('-host', type=str, default='0.0.0.0',
                    help='host address to listen for connections or dgrams')
parser.add_argument('-port', type=int, default=8989,
                    help='host port to listen for connections or dgrams')
parser.add_argument('-type', type=str, default='tcp',
                    help='type of client connections (e.g. web or udp')

parser.add_argument('-fhost', type=str, default='127.0.0.1',
                    help='forwarding host')
parser.add_argument('-fport', type=int, default=6767,
                    help='host port to forward too')
parser.add_argument('-ftype', type=str, default='tcp',
                    help='"udp" uses only host/port or "tcp" (uses more)')
parser.add_argument('-fproto', type=str, default="https",
                    help='tcp uri protocol (e.g. https, http, etc.')

parser.add_argument('-furi', type=str, default='',
                    help='uri protocol part of a request when using tcp')

parser.add_argument('-json_keys_prend', type=str, nargs='+',
                    help='json keys in <key>:[<prepend>] format')

logging.getLogger().setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logging.getLogger().addHandler(ch)


def parse_keys(parser_args):
    key_infor = parser_args.json_keys_prend
    return dict([i.split(':') for i in key_infor])


def run_udp_server(parser_args):
    host, port = parser_args.fhost, parser_args.fport
    my_host, my_port = parser_args.host, parser_args.port
    my_url = parser_args.url

    GeoIPEnricher.set_my_info(my_host, my_port, my_url)
    GeoIPEnricher.set_forward(host, port)
    GeoIPEnricher.set_enrichment_keys(parse_keys(parser_args))
    server = GeoIPEnricher.get_server()
    server.serve_forever(poll_interval=0.5)


def run_tcp_webserver(parser_args):
    my_host, my_port = parser_args.host, parser_args.port
    my_url = parser_args.url

    class MyApplication(web.application):
        def run(self, *middleware):
            func = self.wsgifunc(*middleware)
            # note capturing host and port from above
            return web.httpserver.runsimple(func, (my_host, my_port))

    host, port = parser_args.fhost, parser_args.fport
    uri = parser_args.furi
    proto = parser_args.fproto
    GeoIPEnricher.set_my_info(my_host, my_port, my_url)

    GeoIPEnricher.set_forward(host, port, uri, proto)
    GeoIPEnricher.set_enrichment_keys(parse_keys(parser_args))
    app = web.application(GeoIPEnricher.urls(), globals())
    app.run()


if __name__ == "__main__":
    args = parser.parse_args()
    if parser.type == 'udp':
        logging.debug("Starting the udp listener")
        run_udp_server(args)
    else:
        logging.debug("Starting the tcp listener")
        run_tcp_webserver(args)

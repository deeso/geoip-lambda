import socket
import urllib2
import geoip
from geoip import geolite2
from StringIO import StringIO
import tarfile
import os
import json

'''
This product **uses** GeoLite2 data created by MaxMind, available from
<a href="http://www.maxmind.com">http://www.maxmind.com</a>.
'''

BASE_URL = 'http://geolite.maxmind.com/download/geoip/database'
CITY_URL_LOCATION = '/'.join([BASE_URL, 'GeoLite2-City.tar.gz'])
ASN_URL_LOCATION = '/'.join([BASE_URL, 'GeoLite2-ASN.tar.gz'])
COUNTRY_URL_LOCATION = '/'.join([BASE_URL, 'GeoLite2-Country.tar.gz'])

CITY_MMDB = 'GeoLite2-City.mmdb'
ASN_MMDB = 'GeoLite2-ASN.mmdb'
COUNTRY_MMDB = 'GeoLite2-Country.mmdb'

MMDBS = [CITY_MMDB, ASN_MMDB, COUNTRY_MMDB]
DEFAULT_SAVELOC = "/tmp/"


class Lookups(object):
    def __init__(self, asn_mmdb_location=DEFAULT_SAVELOC,
                 asn_url_location=ASN_URL_LOCATION,
                 country_mmdb_location=DEFAULT_SAVELOC,
                 country_url_location=COUNTRY_URL_LOCATION,
                 city_mmdb_location=DEFAULT_SAVELOC,
                 city_url_location=CITY_URL_LOCATION,
                 reload_if_present=False,
                 use_internal_api=False):

            self.asn_url_location = asn_url_location
            self.city_url_location = city_url_location
            self.country_url_location = country_url_location

            self.asn_mmdb_location = asn_mmdb_location
            self.city_mmdb_location = city_mmdb_location
            self.country_mmdb_location = country_mmdb_location

            self.asn_db = None
            self.city_db = None
            self.country_db = None

            self.loaded = False
            self.reloading = False
            self.use_internal_api = use_internal_api

            if self.use_internal_api:
                self.load_all_dbs(reload_if_present=reload_if_present)

    @classmethod
    def get_geoip_file(cls, url_location,
                       save_loc=DEFAULT_SAVELOC, target=None):
        print "Downloading file %s" % url_location
        data = urllib2.urlopen(url_location).read()
        tar = tarfile.open(mode="r:gz", fileobj=StringIO(data))
        found_name = None
        t_filename = None
        for fullname in tar.getnames():
            _, name = os.path.split(fullname)
            if name in MMDBS:
                t_filename = fullname
                found_name = name
                break
        if found_name is None:
            raise Exception("Unable to download the file")

        # print ("Target= %s and files: %s" % (target, tar.getnames()))

        if target is not None and target != found_name:
            raise Exception("Incorrect file")

        file_data = tar.extractfile(t_filename).read()
        filename = os.path.join(save_loc, found_name)
        open(filename, 'wb').write(file_data)

    @classmethod
    def download_file(cls, save_loc, target, download=False):
        if not os.path.exists(os.path.join(save_loc, target)):
            return True
        if os.path.exists(os.path.join(save_loc, target)) and download:
            return True
        return False

    def load_geoip_asn(self, url_location=ASN_URL_LOCATION,
                       save_loc=DEFAULT_SAVELOC, download=False):
        target = ASN_MMDB
        if self.download_file(save_loc, target, download=download):
            self.get_geoip_file(url_location=url_location,
                                save_loc=save_loc,
                                target=target)

        location = os.path.join(save_loc, target)
        self.asn_db_location = save_loc
        self.asn_db = geoip.open_database(location)

    def load_geoip_country(self, url_location=COUNTRY_URL_LOCATION,
                           save_loc=DEFAULT_SAVELOC, download=False):
        target = COUNTRY_MMDB
        if self.download_file(save_loc, target, download=download):
            self.get_geoip_file(url_location=url_location,
                                save_loc=save_loc,
                                target=target)

        location = os.path.join(save_loc, target)
        self.country_db_location = save_loc
        self.country_db = geoip.open_database(location)

    def load_geoip_city(self, url_location=CITY_URL_LOCATION,
                        save_loc=DEFAULT_SAVELOC, download=False):
        target = CITY_MMDB
        if self.download_file(save_loc, target, download=download):
            self.get_geoip_file(url_location=url_location,
                                save_loc=save_loc,
                                target=target)

        location = os.path.join(save_loc, target)
        self.city_db_location = save_loc
        self.city_db = geoip.open_database(location)

    def load_all_dbs(self, reload_if_present=False):

        self.load_geoip_asn(save_loc=self.asn_mmdb_location,
                            url_location=self.asn_url_location,
                            download=reload_if_present)

        self.load_geoip_city(save_loc=self.city_mmdb_location,
                             url_location=self.city_url_location,
                             download=reload_if_present)

        self.load_geoip_country(save_loc=self.country_mmdb_location,
                                url_location=self.country_url_location,
                                download=reload_if_present)
        self.loaded = True

    def reload(self):
        self.reloading = True
        self.load_all_dbs(reload_if_present=True)
        self.reloading = False

    def lookup(self, host_ip):
        if self.use_internal_api:
            return self.lookup_internal(host_ip)
        return geolite2.lookup(host_ip)

    @classmethod
    def update_results(cls, a, b):
        for k, v in b.items():
            if k in a and a[k] is None:
                a[k] = v
            elif k not in a:
                a[k] = v
            elif isinstance(a[k], frozenset) and isinstance(a[k], frozenset):
                a[k] |= v
        return a

    @classmethod
    def serializable_result(cls, results):
        _results = {}
        for k, v in results.items():
            if not isinstance(v, frozenset):
                _results[k] = v
            else:
                _results[k] = [i for i in v]
        return json.dumps(_results)

    def lookup_internal(self, host_ip):
        if not self.loaded:
            raise Exception("GeoIP database not loaded")

        results = {}
        r = self.city_db.lookup(host_ip)
        if r is not None:
            results = self.update_results(results, r.to_dict())

        r = self.country_db.lookup(host_ip)
        if r is not None:
            results = self.update_results(results, r.to_dict())

        r = self.asn_db.lookup(host_ip)
        if r is not None:
            results = self.update_results(results, r.to_dict())

        return self.serializable_result(results)

    def lookup_name(self, hostname):
        host_ip = None
        try:
            host_ip = socket.gethostbyname(hostname)
        except:
            host_ip = hostname

        return self.lookup(host_ip)

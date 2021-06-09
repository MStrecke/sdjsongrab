#!/usr/bin/env python3
#-*- coding: utf-8 -*-
#
# (c) 2021 by Michael Strecke
# this program is licensed under the GPLv3, see LICENSE for details

import urllib.request
import sqlite3
import datetime
import json
import os
import sys
import time
import configparser
import zlib, gzip
import argparse
import io
import traceback

from lib.util import sdtime_to_unixtime, date_to_int, chunker, int_to_datetime, int_to_date
from lib.sqlhelper import Query_builder, local_cursor_wrapper

__version__ = "0.3"
UTILNAME = "sdjsongrab.py"

USERAGENT = "%s/%s" % (UTILNAME, __version__)

CURRENT_DATABASE_SCHEMA_VERSION = 0

MAX_STATIONS_IDS = 5000
MAX_PROGRAM_IDS = 5000
MAX_ARTWORK_IDS = 500

CONFIG_FNM = 'sdjson.ini'

CONFIG_DEFAULTS = {
    'access': {
        'username': '',
        'xpassword': ''
    },
    'database': {
        'filename': "sd.db"
    },
    'debug' : {
        'maindir': "debug",
        'basefilename': "debug.txt",
        'active': False
    },
    'art' :  {
        'programs': 'program_art'
    },
    'log' : {
        'filename': 'sdjsongrab.log'
    },
    'server': {
        'waituntil': 0
    }
}

SDDB_SCHEMA = "sql/database.sql"

SD_API = "20141201"
SD_URI = "https://json.schedulesdirect.org"   # no trailing "/"

# mo trailing "/"
API_API = "/" + SD_API
API_AVAILABLE  = "/" + SD_API + "/available"
API_VERSION = "/" + SD_API + "/version"
API_TOKEN = "/" + SD_API + "/token"
API_GET_STATUS = "/" + SD_API + "/status"
API_HEADENDS = "/" + SD_API + "/headends"
API_LINEUPS = "/" + SD_API + "/lineups"
API_TRANSMITTERS = "/" + SD_API + "/transmitters"
API_PROGRAMS = "/" + SD_API + "/programs"
API_PROGRAMS_ARTWORK = "/" + SD_API + "/metadata/programs"
API_SCHEDULES = "/" + SD_API + "/schedules"
API_GENERIC_DESCRIPTION = "/" + SD_API + "/metadata/description/"

class SD_Config:
    def __init__(self, cfgfnm):
        self.cfgfnm = cfgfnm
        self.cfg = configparser.ConfigParser()

        if os.path.exists(cfgfnm):
            # if file exists get ALL information from there
            self.cfg.read(cfgfnm)
        else:
            # only if files does not exists use defaults and write INI file
            self.cfg.read_dict(CONFIG_DEFAULTS)
            with open(cfgfnm, "w", encoding="utf8") as configfile:
                self.cfg.write(configfile)

        self.logfile = None

        if 'log' in self.cfg.keys():
            logfnm = self.cfg['log'].get("filename")
            if logfnm not in [None, '']:
                self.logfile = open(logfnm, "a", encoding="utf8")

    def get_str(self, *keys, default=None):
        top = self.cfg
        for key in keys:
            if not key in top.keys():
                return default
            top = top[key]
        return top

    def get_int(self, *keys, default=None):
        w = self.get_str(*keys, default=default)

        if w is default:
            return default

        try:
            return int(w)
        except ValueError:
            return default


    def print_and_log(self, *args, flush=False):
        s = " ".join([str(x) for x in args])
        print(s)

        if self.logfile is not None:
            self.logfile.write(int_to_datetime(time.time(), "%Y-%m-%d %H:%M:%S: "))
            self.logfile.write(s+'\n')
            if flush:
                self.logfile.flush()

    def log_flush(self):
        if self.logfile is not None:
            self.logfile.flush()

    def log_close(self):
        if self.logfile is not None:
            self.logfile.close()
        self.logfile = None

    def write(self):
        with open(self.cfgfnm, 'w', encoding="utf8") as configfile:
            self.cfg.write(configfile)

    def get_username_xpassword(self):
        return self.cfg["access"]["username"], self.cfg["access"]["xpassword"]

    def set_username_xpassword(self, username, xpassword):
        self.cfg["access"]["username"] = username
        self.cfg["access"]["xpassword"] = xpassword

    def get_database_filename(self):
        return self.cfg["database"]["filename"]

    def get_debug_maindir(self):
        return self.cfg["debug"]["maindir"]

    def get_station_rename(self):
        """ get dict to rename station

        :return: dict: key=stationID, name=new name; set: name of all stations
        :rtype: dict, set
        """
        if "stationrename" not in self.cfg.sections():
            return None

        rename_dict = dict()
        station_names = set()
        for key in self.cfg["stationrename"]:
            sn = self.cfg["stationrename"][key]
            rename_dict[key] = sn
            station_names.add(sn)

        if len(rename_dict) == 0:
            return None

        return rename_dict, station_names


    def get_debug_basefilename(self):
        return self.cfg["debug"]["basefilename"]

    def get_debug_active(self):
        return self.cfg.getboolean("debug", "active", fallback=False)

class DebugWriter:
    def __init__(self, maindir, basefilename):
        self.maindir = maindir
        self.basefilename = basefilename
        self.out = None


    def close(self):
        if self.out is not None:
            self.out.close()
        self.out = None

    def flush(self):
        if self.out is not None:
            self.out.flush()

    def enable_logging(self, dolog):
        if dolog:
            if self.out is None:
                now = datetime.datetime.now()
                debugpath = os.path.join(self.maindir, now.strftime("%Y%m%d"))
                os.makedirs(debugpath, exist_ok=True)

                # add timestamp to debug filename
                stamp = now.strftime("_%Y-%m-%d_%H%M%S")
                dbparts = os.path.splitext(self.basefilename)
                debugfnm = os.path.join(debugpath, dbparts[0] + stamp + dbparts[1])
                print("* Logging to: ", debugfnm)

                self.out = open(debugfnm, "w", encoding="utf8")
        else:
            self.close()


    def write_debug(self, s):
        if self.out is not None:
            self.out.write(s)
            self.out.write("\n")



class SD_API(DebugWriter):
    """
    This class provides access to th SD API.

    Two items will be cached and used:
    - the API token when retrieved by get_token
    - the status object when retrieved by get_status

    The class can log the communication to a text file for debuging purposes.
    """

    def __init__(self, config, debug=False, quiet=False):
        """

        :param debug: log to debug file
        :param quiet: only log to file
        """
        DebugWriter.__init__(self, config.get_debug_maindir(),
                             config.get_debug_basefilename())

        self.token = None                # filled by get_token
        self.status = None               # filled by get_status
        self.lineup_uri_mapping = {}     # filled by get_status

        self.config = config
        self.debug = debug     # log to file/screen
        self.quiet = quiet     # only log to file
        self.out = None        # log file handle

        self.enable_logging(self.debug)

    def debugout(self, s):

        if self.debug:
            if not self.quiet:
                print(s)
            self.write_debug(s)


    def show_error(self, wcode, dat):
        """ output more info if error was detected

        :param wcode: error code
        :param dat: data returned from API call
        :return:
        """
        if dat is None:
            return

        if wcode not in [0, None]:
            print("*** error", wcode)

            if wcode > 0:
                response = dat.get('response')
                if response is not None:
                    print(response)
                metadata = dat.get('metadata')
                if metadata is not None:
                    message = metadata.get('message')
                    if message is not None:
                        print(message)
        if dat == []:
            print("** result is empty list")
            return

        if not (type(dat) in [dict, list]):
            print("unknown data type", dat)
            return

        if wcode not in [None, 0]:
            resp = dat.get('response')
            msg = dat.get('message')
            print("%s: %s" % (wcode, resp))
            if msg is not None:
                print(msg)

    @staticmethod
    def hash_password(password):
        import hashlib
        return hashlib.sha1(password.encode("utf8")).hexdigest()

    def get_url(self, verb, url, data=None, headers=None):
        headers2 = {
            'User-Agent': USERAGENT,
            'Accept-Encoding': "identity, gzip, deflate"   # allow compression
        }

        if headers is not None:
            headers2.update(headers)

        verb = verb.upper()
        assert verb in ['GET', 'PUT', 'POST', 'DELETE']

        rq = urllib.request.Request(url, headers=headers2, data=data, method=verb)

        try:
            r = urllib.request.urlopen(rq)
            webstatus = r.code
            self.debugout("status code: %s" % r.code)
            self.debugout("headers: %s" % r.getheaders())

        except urllib.error.HTTPError as e:
            r = None
            webstatus = e.code
            self.debugout("status code: %s - %s" % (webstatus, e.reason))

        rdat = None

        if (webstatus // 100) == 2:
            rcv_dat = r.read()
            data_encoding = r.getheader('Content-Encoding')

            if data_encoding == 'gzip':
                rdat = gzip.decompress(rcv_dat)
            elif data_encoding == "deflate":
                rdat = zlib.decompress(rcv_dat)
            else:
                rdat = rcv_dat

        return webstatus, rdat, r

    def api_access(self, verb, apicall, data=None, tail="", use_token=False, show_non_zero_code=True):
        """ send and (log) API call

        :param verb: get, put, post, ....
        :param apicall: URI to use
        :param data: data to send as JSON in body
        :param tail: appended to API call uri
        :param use_token: set token in header
        :return: data from API call or None
        """
        def redact(d):
            import copy
            if d is None:
                return None

            dc = copy.copy(d)
            for key in ['password', 'token']:
                if key in dc:
                    dc[key] = '-- REDACTED --'
            return dc

        headers = dict()

        if use_token and self.token is not None:
            headers['token'] = self.token

        url = SD_URI + apicall + tail

        self.debugout("url: %s" % url)
        self.debugout("verb: %s" % verb)
        self.debugout("headers: %s" % redact(headers))
        self.debugout("data: %s" %  redact(data))
        if type(data) is list:
            self.debugout("  --> # entries: %s" % len(data))

        if data is None:
            dataj = None
        else:
            dataj = json.dumps(data).encode("utf8")
            headers['Content-Type'] = "application/json;charset=UTF-8"

        self.flush()

        webstatus, rdat, r = self.get_url(verb, url, data=dataj, headers=headers)

        rc = None    # result code, 0: ok, None: no code, positive: SD error, negative: HTTP error
        dat = None

        if (webstatus // 100) == 2:
            content_type = r.getheader('Content-Type')

            if content_type is not None and content_type.lower().startswith("application/json"):
                # application/json;charset=UTF-8
                charset = "utf8"    # default
                for item in content_type.split(";"):
                    subitem = item.split("=")
                    if len(subitem) == 2:
                        if subitem[0].lower() == "charset":
                            charset = subitem[1]

                dat = json.loads(rdat.decode(charset))
                j = json.dumps(redact(dat), sort_keys=False,
                            indent=4, separators=(',', ': '))
                self.debugout("json:\n%s\n" % j)

                if type(dat) is list:
                    rc = None
                else:
                    rc = dat.get('code')


            else:
                self.debugout("raw:", rdat)
                dat = rdat

        else:
            rc = -webstatus

        self.debugout("===========")

        self.flush()

        if rc != 0 and show_non_zero_code:
            self.show_error(rc, dat)  # display more info on error

        return dat

    def check_version(self, utilname):
        return self.api_access("get", API_VERSION + "/" + utilname, show_non_zero_code=False)


    def check_newer_version_available(self):
        # error 1005: UNKNOWN_CLIENT
        # ok: 'version': current version
        dat = self.check_version(UTILNAME)
        if dat is None:
            return False
        if self.debug:
            print("%s: %s - %s" % (dat["code"], dat["response"], dat["message"]))
        if dat["code"] != 0:
            return False
        return dat["version"] > __version__

    def get_status(self):
        """ get status and stores it (also) internally

        :return: status object
        """
        """
        Example result

        {
        'account': {
            'expires': '2019-01-09T12:18:31Z',
            'messages': [],
            'maxLineups': 4},
        'lineups': [
            {
                'lineup': 'ZZZ-28.2E-DEFAULT',
                'modified': '2015-04-15T21:11:12Z',
                'uri': '/20141201/lineups/ZZZ-28.2E-DEFAULT',
                'name': 'Freesat - Astra 28.2E'
            }
        ],
        'lastDataUpdate': '2019-01-01T22:26:20Z',
        'notifications': [],
        'systemStatus': [
            {
                'date': '2015-09-08T00:00:00Z',
                'status': 'Online',
                'message': 'No known issues.'
            }
        ],
        'serverID': '20141201.web.X',
        'datetime': '2019-01-02T12:58:21Z',
        'code': 0
        }
        """
        dat = self.api_access("get", API_GET_STATUS, use_token=True)
        self.status = dat      # store status object

        # extract mapping from lineup ID to its URI
        for ldat in dat["lineups"]:
            self.lineup_uri_mapping[ldat["lineup"]] = ldat["uri"]

        return dat

    def has_expired(self):
        assert self.status is not None

        print(self.status["account"]["expires"])
        exp = sdtime_to_unixtime(self.status["account"]["expires"])

        return exp < time.time()



    def check_status(self):
        all_ok = True

        assert self.status is not None

        if self.status["code"] != 0:
            all_ok = False
            self.config.print_and_log("*** status_error code", self.status["code"])

        if self.status["notifications"] != []:
            self.config.print_and_log("*** Notifications:")
            for msg in self.status["notifications"]:
                self.config.print_and_log(msg)

        for s in self.status["systemStatus"]:
            if s["status"] != "Online":
                all_ok = False
                self.config.print_and_log("*** System status: %s, %s" % (
                  s["status"],
                  s["message"]
                  ))

        self.config.log_flush()

        return all_ok

    def calc_lineups_and_modified_date_from_status(self):
        return [
            (x["lineup"], sdtime_to_unixtime(x["modified"])) for x in self.status["lineups"]
        ]

    def get_token(self, username, xpassword):
        """ requests a token and stores it internally

        :param username: user name of user
        :param xpassword: hashed password of user
        :return: token object
        """
        """
        Example result

        {
            "code": 0,
            "message": "OK",
            "serverID": "20141201.web.X",
            "datetime": "2019-01-02T20:02:15Z",
            "token": "293330b5053834084d7ac87d26700a20"
        }
        """
        dat = self.api_access("post", API_TOKEN, {'username': username, 'password': xpassword})
        if dat is not None:
            if dat["code"] != 0:
                self.config.print_and_log("* error obtaining token:", dat["code"], dat["message"], flush=True)
                sys.exit(1)
            self.token = dat["token"]
        return dat

    def get_lineup_mapping(self, lineup_id):
        if lineup_id is None:
            return None

        return self.lineup_uri_mapping.get(lineup_id)

    # GET https://json.schedulesdirect.org/20141201/headends?country=GBR&postalcode=XX00
    def get_headends(self, countrycode, postalcode):
        return self.api_access("get", API_HEADENDS, use_token=True, tail="?country=%s&postalcode=%s" % (countrycode, postalcode))

    def get_country_transmitters(self, country_code):
        return self.api_access("get", API_TRANSMITTERS, tail="/"+country_code)

    def get_user_lineups(self):
        """
        Example result

        {
            "code": 0,
            "serverID": "20141201.web.X",
            "datetime": "2019-01-02T20:02:17Z",
            "lineups": [
                {
                    "lineup": "ZZZ-28.2E-DEFAULT",
                    "name": "Freesat - Astra 28.2E",
                    "transport": "DVB-S",
                    "location": "28.2E",
                    "uri": "/20141201/lineups/ZZZ-28.2E-DEFAULT"
                }
            ]
        }
        """
        return self.api_access("get", API_LINEUPS, use_token=True)

    # GET https://json.schedulesdirect.org/20141201/lineups/preview/USA-IL57303-X
    def preview_lineup(self, lineupid):
        return self.api_access("get", API_LINEUPS, use_token=True, tail="/preview/" + lineupid)

    # PUT /lineups/{COUNTRY}-{LINEUP}-{DEVICE}
    def add_lineup_to_user_account(self, lineupid):
        return self.api_access("put", API_LINEUPS, use_token=True, tail="/" + lineupid)

    # PUT /lineups/{COUNTRY}-{LINEUP}-{DEVICE}
    def delete_lineup_from_user_account(self, lineupid):
        return self.api_access("delete", API_LINEUPS, use_token=True, tail="/" + lineupid)

    def get_lineup_stations(self, lineupuri):
        return self.api_access("get", lineupuri, use_token=True, tail="")

    def get_schedules(self, station_ids):
        """ return schedules for programm_ids

        :param program_ids: list of SD program_ids
        :return:
        """
        assert len(station_ids) <= 500

        if type(station_ids) is list:
            dat = [{'stationID': x} for x in station_ids]
        elif type(station_ids) is dict:
            dat = []
            for x in station_ids.keys():
                dat.append(
                    {
                        'stationID': x,
                        'date': station_ids[x]
                    }
                )

        return self.api_access("post", API_SCHEDULES, data=dat, use_token=True)

    def get_schedules_md5_max(self, station_ids):
        """ return MD5 for schedules

        :param program_ids: list of SD program_ids
        :return:
        """
        """
        Example result
        {
            "10021": {
                "2015-03-02": {
                    "code": 0,
                    "message": "OK",
                    "lastModified": "2015-03-02T15:54:58Z",
                    "md5": "90OhzuWDRZ/3pDA8kiD/3Q"     # or "CAFEDEADBEEFCAFEDEADBE"
                },
                "2015-03-03": {
                    "code": 0,
                    "message": "OK",
                    "lastModified": "2015-03-02T15:54:58Z",
                    "md5": "SpVOfFY4a8gQrrZjOqyK8g"
                },
                ...
        """

        assert len(station_ids) <= MAX_STATIONS_IDS

        dat = [ {'stationID': x} for x in station_ids]

        return self.api_access("post", API_SCHEDULES, data=dat, tail="/md5", use_token=True)

    def get_programs_max(self, program_ids):
        """ get program info

        :param program_ids: list of SD program_ids
        :return:
        """
        assert len(program_ids) <= MAX_PROGRAM_IDS

        return self.api_access("post", API_PROGRAMS, data=program_ids, use_token=True)

    def shorten_program_id(self, program_id):
        return program_id[:10]

    def calc_short_program_ids(self, program_ids):
        shortend_progids = []
        for item in program_ids:
            # only the leftmost 10 chars
            short = self.shorten_program_id(item)
            if not short in shortend_progids:
                shortend_progids.append(short)
        return shortend_progids

    def get_program_artwork_max(self, short_program_ids):
        return self.api_access("post", API_PROGRAMS_ARTWORK, data=short_program_ids, use_token=False)

    def calc_artwork_uri(self, uri):
        if not(uri.startswith("http:") or uri.startswith("https:")):
            uri = "https://json.schedulesdirect.org/20141201/image/" + uri
        return uri

    def get_artwork(self, uri):
        full_uri = self.calc_artwork_uri(uri)
        webcode, rdat, rr = self.get_url("GET", full_uri)
        if webcode // 100 != 2:
            return None, None

        return rdat, rr

    def proc_program_artwork_info(self, data):
        """ search best fitting URL

        :param data: data from API_PROGRAMS_ARTWORT
        :type data: [type]
        :result: dict: shortened_program_id -> (uri, caption or None, width or None, height or None)
        :note: The result dict uses shortened IDs!
        """
        def best_match(current_list, preferred_list):
            """ search best match (item from proposed list in current list)

            :param current_list: list of read (real) values
            :type current_list: list-like of str
            :param preferred_list: list of preferred values (most preferred first)
            :type preferred_list: list of str
            :return: best matching real entry
            :rtype: str
            """
            if current_list == []:
                return None

            for item in preferred_list:
                if item in current_list:
                    return item              # preferred entry found, return it

            return list(current_list)[0]     # as a default: return first entry from current list

        res = {}

        for program_art in data:
            program_id_short = program_art["programID"]
            dat = program_art["data"]

            if type(dat) != list:         # error
                continue

            # create asset dict
            # - first key is content type
            # - second key is image size
            # - value is (uri, caption or None)
            assets = {}
            for item in dat:
                caption = item.get('caption')
                if caption is not None:
                    caption = caption.get('content')

                # use only entries with category type and size
                category = item.get('category')
                size = item.get('size')
                if category is None or size is None:
                    continue

                # prepend SD asset URI is url is not complete
                uri = item["uri"]
                if not(uri.startswith("http:") or uri.startswith("https:")):
                    uri = "https://json.schedulesdirect.org/20141201/image/" + uri

                width = item.get("width")
                height = item.get("height")

                # create new entry for new category
                if not category in assets:
                    assets[category] = {}

                # create new entry within category for new size
                if not size in assets[category]:
                    assets[category][size] = []

                # save uri + caption
                assets[category][size].append((uri, caption, width, height))

            # select best match
            assetskeys = assets.keys()
            if assetskeys == []:
                continue

            best_cat = best_match(
                assetskeys, ['Iconic', 'Banner-L1', 'Cast Ensemble', 'Cast in Character'])
            best_size = best_match(assets[best_cat].keys(), [
                                'Md', 'Sm', 'Lg', 'Xs', 'Ms'])

            res[program_id_short] = assets[best_cat][best_size][0]
        return res

    def calc_program_artwork_info(self, program_id, artworkinfo):
        """ search entry from artworkinfo (dict of URLs)

        :param program_id: programID
        :type program_id: str
        :param artworkinfo: dict with (best fitting) artwork URLs
        :type artworkinfo: dict
        :return: image URL
        :rtype: str or None
        """
        return artworkinfo.get(self.shorten_program_id(program_id))

    def available_services(self):
        return self.api_access("get", API_AVAILABLE)    # NB: response hat no 'code' entry -> code = None

def get_from_simple_dict_list(datadict, searchkeys, maxlength):
    """ get from simple dict

    :param datadict: data dict
    :type datadict: dict or None
    :param searchkeys: key within dict
    :type searchkeys: list of str
    :param maxlength: if key is not found any string with max length
    :type maxlength: int
    :return: string or None
    """
    """
    Example
    "titles": [
        {
            "title120": "'Allo 'Allo!"
        }
    ],
    """
    if datadict is None:
        return None
    assert type(datadict) is list

    result = None

    if searchkeys is not None:
        # search dict for searchkey
        for item in datadict:
            for key in searchkeys:
                a = item.get(key)
                if a is not None:
                    result = a
                    break
            if result is not None:
                break

    if result is None:
        # longest value from any entry that is shorter than maxlen
        curlen = None
        for item in datadict:
            for anykey in item.keys():
                a = item[anykey]
                if maxlength is None or (len(a) <= maxlength):
                    if (curlen is None) or (len(a) > curlen):
                        result = a
                        curlen = len(a)

    return result

def get_from_dict_list_with_languages(datadict, searchkeys, languagekey, valuekey, languages):
    """ get entry with language from dirct

    :param datadict: dict from overall data
    :type datadict: dict or None
    :param searchkeys: desired key within that dict
    :type searchkeys: str
    :param languagekey: name of the key holding the language
    :type languagekey: str
    :param valuekey: name of the key holding the value
    :type valuekey: str
    :param languages: desired languages
    :type languages: list of str
    :return: value
    :rtype: str or None
    """

    """
    "descriptions": {
        "description100": [
            {
                "descriptionLanguage": "en-GB",
                "description": "Batwoman has a random encounter that forces her to revisit her painful past."
            }
        ],
        "description1000": [
            {
                "descriptionLanguage": "en-GB",
                "description": "As Batwoman attempts to fight the proliferation of Snake Bite through Gotham, a random encounter forces her to revisit her painful past; Ryan Wilder is determined to ensure others like her don't go unnoticed."
            }
        ]
    },
    """
    if datadict is None:
        return None
    assert type(datadict) is dict

    if searchkeys is None:
        return
    assert type(searchkeys) is list

    result = None
    default_value = None

    # scan every item in the list
    for itemkey in datadict.keys():
        # eg. descriptions100, descriptions1000

        if itemkey in searchkeys:

            for item in datadict[itemkey]:   # list of dict with valuekey + languagekey
                value = item.get(valuekey)
                lang = item.get(languagekey)

                if value is None or lang is None:
                    # must have both
                    continue

                lang = lang.lower()         # make it lower case for easier comparison

                # get english value as a last resort
                if lang == "en" or lang.startswith("en-"):
                    default_value = value

                # check item language against user provided langauge list
                for lkey in languages:
                    # exact or start, e.g. "en", "en-gb"
                    if (lang == lkey) or lang.startswith(lkey+"-"):
                        result = value
                        break

    if result is None:
        result = default_value

    return result

def calc_string_from_stringlist(stringlist):
    """
        "genres": [
            "Drama",
            "Fantasy"
        ],
    """
    if stringlist is None:
        return None
    return "\t".join(stringlist)

def calc_episode_number(list_of_dicts):
    if list_of_dicts is None:
        return None

    season = None
    episode = None
    for item in list_of_dicts:
        if "Gracenote" in item.keys():
            p = item["Gracenote"]
            season = p.get("season")
            episode = p.get("episode")
        if season is not None and episode is not None:
            break
    if season is None or episode is None:
        return None
    return "%dx%02d" % (season, episode)


def _get_cast_crew_string(list_of_dicts, name_key, role_key):
    """ create compact name/role list string

    :param list_of_dicts: data for episode
    :type list_of_dicts: dict
    :param name_key: name of name key
    :type name_key: str
    :param role_key: name of role key
    :type role_key: str
    :return: compact data string for database
    :rtype: str or None
    """


    if list_of_dicts is None:
        return None

    clist = []
    for item in list_of_dicts:
        s = item[name_key]
        s1 = item.get(role_key)
        if s1 is not None:        # has role -> "name|role"  otherwise only  "name"
            s = s + "|" + s1
        clist.append(s)

    if clist == []:
        return None
    return "\t".join(clist)      # join all entries with tab


def get_casts_string(list_of_dicts):
    return _get_cast_crew_string(list_of_dicts, "name", "characterName")


def get_crew_string(list_of_dicts):
    return _get_cast_crew_string(list_of_dicts, "name", "role")


class SD_QueryBuilder(Query_builder):
    def __init__(self, table, data, *, joker='?'):
        Query_builder.__init__(self, table, joker=joker)
        self.data = data
        self.messages = []

    def add2(self, database_key, data_key, *, optional=False, conv_fun=None):
        if not data_key in self.data:
            if optional:
                return
            raise KeyError("data_key " + data_key + " not found")

        s = self.data[data_key]
        if conv_fun is not None:
            s = conv_fun(s)

        self.add(database_key, s)



class SDDB:
    """
        sqlite database to store/cache results from SD
    """
    def __init__(self, sdapi, config):
        self.config = config
        sddb_fnm = self.config.get_database_filename()
        if not os.path.exists(sddb_fnm):
            print("* creating new database:", sddb_fnm)
            schema = open(SDDB_SCHEMA, "r", encoding="utf8").read()
            self.con = sqlite3.connect(sddb_fnm)
            self.con.executescript(schema)
        else:
            self.con = sqlite3.connect(sddb_fnm)

        self.con.row_factory = sqlite3.Row

        self.sdapi = sdapi

        if self.sdapi is None:
            self.debug = False
        else:
            self.debug = self.sdapi.debug

    def commit(self):
        self.con.commit()

    def last_id(self, cu):
        return cu.lastrowid

    def cursor(self):
        return self.con.cursor()

    @local_cursor_wrapper
    def fetchone(self, query, param=None, *, retparam=None, cursor=None):
        """ conveniance function: fetch one data result, if retparam is set -> item, return None if not available

        :param query: SQL query
        :type query: str
        :param param: SQL params, defaults to None
        :type param: tuple, optional
        :param retparam: name of parameter, defaults to None -> normal fetchone
        :type retparam: str, optional
        :param cursor: database cursor, defaults to None
        :type cursor: cursor, optional
        :return: content of item or None
        :rtype: str/intAny or None
        """
        if param is None:
            param = ()
        cursor.execute(query, param)
        res = cursor.fetchone()
        if res is None:
            return None
        if retparam is None:
            return res
        return res[retparam]

    @local_cursor_wrapper
    def get_database_version(self, cursor=None):
        cursor.execute("PRAGMA user_version")
        res = cursor.fetchone()
        return res[0]

    @local_cursor_wrapper
    def upgrade_database_schema(self, current_schema_version, upgrade_plan=None, *, cursor=None):
        """ upgrade database schema

        :param current_schema_version: current version of database schema
        :type current_schema_version: int
        :param upgrade_plan: version and filename of update script, defaults to None
        :type upgrade_plan: list[int, str], optional
        :param cursor: database cursor, defaults to None
        :type cursor: cursor, optional
        :return: all went well
        :rtype: bool

        The upgrade plan is a list of tuples (version_num, filename).  The filename
        describes an SQL script to add new fields.  The script must also set
        the new `PRAGMA user_version`.

        """

        vers = self.get_database_version(cursor=cursor)

        if vers == current_schema_version:
            return True

        if upgrade_plan is None:
            self.config.print_and_log("* Warning: Database needs upgrading but upgrade plan is missing", flush=True)
            return False

        self.config.print_and_log("* Trying to update database schema", flush=True)
        for uvers, ufnm in upgrade_plan:
            if vers < uvers:
                if not os.path.exists(ufnm):
                    self.config.print_and_log("* Warnung: Database needs upgrading but upgrade sql file %s is missing" % (ufnm,), flush=True)
                    return False
                additional_schema = open(ufnm, "r", encoding="utf8").read()
                self.con.executescript(additional_schema)

        self.config.print_and_log("* Database schema successfully upgraded", flush=True)
        return True

    @local_cursor_wrapper
    def print_database_stats(self, *, cursor=None):
        res = self.get_lineups_and_modified_date(cursor=cursor)
        print("\nLineups:")
        print("========")
        print("total:", len(res))
        for item in res:
            print(item[0], " - last modified:", int_to_datetime(item[1], format="%Y-%m-%d %H:%M:%S"))
        numstat = self.fetchone("SELECT count(*) FROM stations", retparam="count(*)", cursor=cursor)
        active_stations = self.fetchone("SELECT count(*) FROM stations WHERE active=?", (1,), retparam="count(*)", cursor=cursor)
        query_stations = self.fetchone(
            "SELECT count(*) FROM stations WHERE query_from_sd=?", (1,), retparam="count(*)", cursor=cursor)
        dangling_stations = self.fetchone(
            "SELECT count(*) FROM stations WHERE query_from_sd=? and active=?", (1, 0), retparam="count(*)", cursor=cursor)

        print("\nStations:")
        print("=========:")
        print("total:", numstat)
        print("query:", query_stations)
        print("active:", active_stations)
        print("invalid query:", dangling_stations)

        numsched = self.fetchone(
            "SELECT count(*) FROM schedule", retparam="count(*)", cursor=cursor)
        oldsched = self.fetchone(
            "SELECT min(startdate) FROM scheduleindex", retparam="min(startdate)", cursor=cursor)
        newsched = self.fetchone(
            "SELECT max(startdate) FROM scheduleindex", retparam="max(startdate)", cursor=cursor)

        print("\nSchedules:")
        print("==========")
        print("total:", numsched)
        print("oldest:", int_to_date(oldsched))
        print("newest:", int_to_date(newsched))

        print("\nPrograms:")
        print("=========")

        numprog = self.fetchone(
            "SELECT count(*) FROM programdata", retparam="count(*)", cursor=cursor)
        print("total:", numprog)

    @local_cursor_wrapper
    def get_lineups_and_modified_date(self, *, cursor=None):
        cursor.execute("SELECT lineupname, modified FROM lineups")
        return [(x["lineupname"], x["modified"]) for x in cursor.fetchall()]

    @local_cursor_wrapper
    def get_active_query_stations(self, withname:bool=False, *, cursor=None):
        """ get list of stationID (withname) that can be queried

        :param withname: add station name in list, defaults to False
        :type withname: bool, optional
        :return: list of stationID (+ station name)
        :rtype: list of str, list of (str, str)
        """

        cursor.execute("SELECT station_id, name FROM stations WHERE query_from_sd=? and active=?", (1,1))
        if withname:
            return [(x["station_id"], x["name"]) for x in cursor.fetchall()]
        else:
            return [x["station_id"] for x in cursor.fetchall()]

    @local_cursor_wrapper
    def set_query_station(self, station_id:str, do_query:bool=True, *, cursor=None) -> str:
        """ set stations do_query flag to True/False

        :param station_id: stationID to update
        :type station_id: str
        :param do_query: query flag defaults to True
        :type do_query: bool
        :return: error/ok message
        :rtype: str
        """
        cursor.execute("SELECT * FROM stations WHERE station_id=?", (station_id,))
        res = cursor.fetchone()
        if res is None:
            return "station %s not found" % station_id
        qv = 1 if do_query else 0
        cursor.execute(
            "UPDATE stations SET query_from_sd=? WHERE station_id=?", (qv, station_id))
        return "ok"

    @local_cursor_wrapper
    def get_active_stations(self, *, cursor=None):
        cursor.execute(
            "SELECT station_id, name FROM stations WHERE active=? ORDER BY name", (1,))
        return [(x["station_id"], x["name"]) for x in cursor.fetchall()]

    @local_cursor_wrapper
    def get_schedule_dates_and_indices(self, station_id, *, cursor=None):
        cursor.execute("SELECT id, startdate FROM scheduleindex WHERE station_id=? ORDER BY startdate", (station_id,))
        return [(x["startdate"], x["id"]) for x in cursor.fetchall()]

    @local_cursor_wrapper
    def get_station_name_from_station_id(self, station_id, *, cursor=None):
        return self.fetchone("SELECT name FROM stations WHERE station_id=?", (station_id,), retparam='name')

    @local_cursor_wrapper
    def store_lineups_from_status(self, data, *, cursor=None):
        new_lineups = []
        for lineup in data["lineups"]:
            qb = SD_QueryBuilder("lineups", lineup)
            qb.add2("lineupname", "lineup")
            qb.add2("name", "name")
            qb.add2("modified", "modified", conv_fun=sdtime_to_unixtime)

            res = qb.insert_or_update(
                cursor,
                "lineupname",                     # insert/update this table
                compare_query_field="modified",   # only update it these differ
                debug=self.debug)

            if res:
                new_lineups.append(lineup)
            if self.debug:
                print("Result store lineup %s from status: %s" % (lineup, res))
        return new_lineups

    @local_cursor_wrapper
    def store_lineups_from_user_lineups(self, data, *, cursor=None):
        for lineup in data["lineups"]:
            qb = SD_QueryBuilder("lineups", lineup)
            qb.add2("lineupname", "lineup")
            qb.add2("name", "name")
            qb.add2("modified", "modified", conv_fun=sdtime_to_unixtime)
            qb.add2("uri", "uri")

            res = qb.insert_or_update(
                cursor,
                "lineupname",                     # insert/update this table (insert/update all)
                debug=self.debug)
            if self.debug:
                print("status store lineups:", res)

    @local_cursor_wrapper
    def deactivate_all_stations(self, *, cursor=None):
        """ set all stations to inactive
            - the subsequent store_lineup_station will set them to active again
        """
        cursor.execute("UPDATE stations SET active=?", (0,))

    @local_cursor_wrapper
    def store_lineup_station(self, data, *, cursor=None):
        stations = data["stations"]
        now = int(time.time())

        for item in stations:
            if type(item) is not dict:
                # Bug https://github.com/SchedulesDirect/JSON-Service/issues/78
                continue

            qb = SD_QueryBuilder("stations", item)
            qb.add2("station_id", "stationID")
            qb.add2("name", "name")
            qb.add2("broadcast_language", "broadcastLanguage", conv_fun=calc_string_from_stringlist)
            qb.add("last_modified", now)
            qb.add("active", 1)
            logo = item.get("logo")
            if logo is not None:
                qb.add("logo_uri", logo["URL"])
                qb.add("logo_width", logo["width"])
                qb.add("logo_height", logo["height"])
                qb.add("logo_md5", logo["md5"])
                #todo: add blob later
                #todo: add blob on change
                #info: logo is kept, even if json entry vanishes

            res = qb.insert_or_update(
                cursor, "station_id", debug=self.debug)
            if self.debug:
                print("Result station insert/update", res)

    @local_cursor_wrapper
    def delete_lineup_from_db(self, lineup_id, *, cursor=None):
        cursor.execute("DELETE FROM lineups WHERE lineupname=?", (lineup_id,))


    @local_cursor_wrapper
    def refresh_lineups_and_stations_if_needed(self, *, cursor=None):
        """ refresh lineups if lineup data in status does not match database

        :param cursor: database cursor, defaults to None
        :type cursor: cursor, optional
        :return: new data has been downloaded from SD
        :rtype: bool
        """
        assert self.sdapi.status is not None

        # compare stored lineup with the one from SD
        old_lineup_data = self.get_lineups_and_modified_date(cursor=cursor)
        new_lineup_data = self.sdapi.calc_lineups_and_modified_date_from_status()

        old_lineup_set = {x[0] for x in old_lineup_data}   # only unique lineupIDs
        new_lineup_set = {x[0] for x in new_lineup_data}   # only unique lineupIDs

        # new/del/same based on lineupID alone
        deleted_lineups = old_lineup_set - new_lineup_set
        new_lineups = new_lineup_set - old_lineup_set
        same_lineups = old_lineup_set.intersection(new_lineup_set)

        # checking timestamp in same_linups
        modified_lineups = set()
        for same_lineup in same_lineups:
            # find lineupID in list of (lineupID, modifiedstamp)
            # -> there can be only one [0]
            # -> get its timestamp [1]
            timeold = list(filter(lambda x: x[0] == same_lineup, old_lineup_data))[0][1]
            timenew = list(filter(lambda x: x[0] == same_lineup, new_lineup_data))[0][1]
            if timeold != timenew:
                modified_lineups.add(same_lineup)

        if self.sdapi.debug:
            print("new lineups", new_lineups)
            print("deleted lineups", deleted_lineups)
            print("modified lineups", modified_lineups)

        if len(deleted_lineups) != 0:
            for lineupID in deleted_lineups:
                self.delete_lineup_from_db(lineupID, cursor=cursor)

        refresh_lineups = (len(new_lineups) != 0) or (len(deleted_lineups) != 0) or (len(modified_lineups) != 0)
        if refresh_lineups:
            # something has changed
            # refresh local lineup
            # if a lineup had been deleted or modified missing stations will simply not being reactivated
            self.store_lineups_from_status(self.sdapi.status, cursor=cursor)

            # and refresh their stations
            self.deactivate_all_stations(cursor=cursor)
            for lineup_item in new_lineup_data:
                dat = self.sdapi.get_lineup_stations(
                    self.sdapi.get_lineup_mapping(lineup_item[0]))
                # this will also (re)activate the station
                self.store_lineup_station(dat, cursor=cursor)

        return refresh_lineups

    @local_cursor_wrapper
    def get_unactive_query_stations(self, *, cursor=None):
        cursor.execute(
            "SELECT station_id, name FROM stations WHERE query_from_sd=1 AND active=0")
        return [(x["station_id"], x["name"]) for x in cursor.fetchall()]

    @local_cursor_wrapper
    def store_schedule_index(self, data, *, cursor=None):
        new_schedules = {}
        count_invalid_schedules = 0
        for station_id in data.keys():
            for startdate in data[station_id]:
                startdate_ord = date_to_int(startdate)
                day_md5 = data[station_id][startdate]["md5"]

                if day_md5 == "CAFEDEADBEEFCAFEDEADBE":
                    count_invalid_schedules += 1
                    continue

                cursor.execute(
                    "SELECT * FROM scheduleindex WHERE station_id=? AND startdate=?", (station_id, startdate_ord))
                res = cursor.fetchone()

                skip_processing = False
                schedindex_to_update = None          # None = new schedule

                if res is not None:
                    if res["md5"] != day_md5:   # schedule changed
                        print("* Schedule changed:" , self.get_station_name_from_station_id(station_id), startdate)
                        schedindex_to_update = res["id"]
                        cursor.execute(
                            "DELETE FROM schedule WHERE scheduleindex=?", (schedindex_to_update,))
                    else:
                        # skip processing only if a schedule did exist in the database AND
                        #  it hasn't changed
                        skip_processing = True

                if not skip_processing:
                    # store new or (previously deleted) modified schedules
                    qb = SD_QueryBuilder(
                        "scheduleindex", data[station_id][startdate])
                    qb.add("station_id", station_id)
                    qb.add("startdate", startdate_ord)
                    qb.add2("last_modified", "lastModified",
                            conv_fun=sdtime_to_unixtime)
                    qb.add2("md5", "md5")

                    if schedindex_to_update is None:
                        res = qb.execute_insert(cursor, debug=self.debug)
                    else:
                        res = qb.execute_update(
                            cursor, "id=?", (schedindex_to_update,))

                    if not station_id in new_schedules:
                        new_schedules[station_id] = []
                    new_schedules[station_id].append(startdate)

        return new_schedules, count_invalid_schedules

    @local_cursor_wrapper
    def get_schedule_index(self, station_id, startdate, *, cursor=None):
        """ get schedule ID for station/date

        :param station_id: stationID
        :type station_id: str
        :param startdate: date
        :type startdate: int
        :param cursor: database cursor, defaults to None
        :type cursor: cursor, optional
        :return: ID of scheduleindex
        :rtype: int or None
        """
        startdate_org = date_to_int(startdate)
        return self.fetchone("SELECT id FROM scheduleindex WHERE station_id=? AND startdate=?",
                    (station_id, startdate_org),
                    retparam="id",
                    cursor=cursor)

    @local_cursor_wrapper
    def store_schedules(self, data, *, cursor=None):
        """ store schedule and get programIDs to download

        :param data: dict with schedule information
        :type data: list of dicts
        :param cursor: database cursor, defaults to None
        :type cursor: cursor, optional
        :return: new/modified programIDs
        :rtype: list of str
        """
        update_programs = []

        for item in data:
            startdate = item["metadata"]["startDate"]

            # print("get schedule index:", item["stationID"], startdate)
            schedule_index = self.get_schedule_index(item["stationID"], startdate, cursor=cursor)
            assert schedule_index is not None

            for program in item["programs"]:
                # print("program in schedule:", program)
                program_id = program["programID"]
                program_md5 = program["md5"]

                qb = SD_QueryBuilder("schedule", program)
                qb.add("station_id", item["stationID"])
                qb.add("scheduleindex", schedule_index)
                qb.add("program_id", program_id)
                qb.add2("airtime", "airDateTime", conv_fun=sdtime_to_unixtime)
                qb.add2("duration", "duration")
                qb.add2("audioProperties", "audioProperties", optional=True, conv_fun=calc_string_from_stringlist)
                qb.add2("videoProperties", "videoProperties", optional=True, conv_fun=calc_string_from_stringlist)

                qb.execute_insert(cursor)

                # do we have this program in cache or has it changed?
                pmd5 = self.fetchone(
                    "SELECT md5 FROM programdata WHERE program_id=?",
                    (program_id,),
                    retparam="md5",
                    cursor=cursor)
                if pmd5 is None or pmd5 != program_md5:
                    if program_id not in update_programs:
                        update_programs.append(program_id)

        return update_programs

    @local_cursor_wrapper
    def get_program(self, program_id, *, cursor=None):
        """ get program and note last access

        :param program_id: programID
        :type program_id: str
        :param cursor: database cursor, defaults to None
        :type cursor: cursor, optional
        :return: program info
        :rtype: dict-like or None
        """
        if program_id is None:
            return None

        cursor.execute("SELECT * FROM programdata WHERE program_id=?", (program_id,))
        res = cursor.fetchone()
        if res is None:
            return None

        curtime = int(time.time())
        cursor.execute(
            "UPDATE programdata SET last_access=? WHERE program_id=?", (curtime, program_id))
        return res

    @local_cursor_wrapper
    def store_programs(self, data, artwork_info=None, *, cursor=None):
        for item in data:
            now = int(time.time())

            qb = SD_QueryBuilder("programdata", item)
            qb.add2("md5", "md5")
            qb.add2("program_id", "programID")
            qb.add("last_access", now)

            title120 = get_from_simple_dict_list(
                item.get("titles"),
                ["title120"],
                120
            )
            qb.add("title120", title120)

            description100 = get_from_dict_list_with_languages(
                item.get("descriptions"),
                searchkeys=["description100"],
                languagekey="descriptionLanguage",
                valuekey="description",
                languages=["en", "de"])
            qb.add("description100", description100)

            description1000 = get_from_dict_list_with_languages(
                item.get("descriptions"),
                searchkeys=["description1000"],
                languagekey="descriptionLanguage",
                valuekey="description",
                languages=["en", "de"])
            qb.add("description1000", description1000)

            qb.add2("genres100", "genres", optional=True, conv_fun=calc_string_from_stringlist)

            episodetitle158 = item.get("episodeTitle150")
            if episodetitle158 is not None:
                episode_number = calc_episode_number(item.get("metadata"))
                if episode_number is not None:
                    episodetitle158 = episode_number + ": " + episodetitle158
                qb.add("episodetitle158", episodetitle158)

            qb.add2("cast1000", "cast", optional=True, conv_fun=get_casts_string)
            qb.add2("crew1000", "crew", optional=True,
                    conv_fun=get_crew_string)

            qb.add2("entity_type", "entityType", optional=True)      # Movie, Episode
            qb.add2("show_type", "showType", optional=True)          # Series, Feature Film

            m = item.get("movie")
            if m is not None:
                y = m.get("year")
                if y is not None:
                    qb.add("movie_year", y)

            if artwork_info is not None:
                art = self.sdapi.calc_program_artwork_info(item["programID"], artwork_info)
                if art is not None:
                    qb.add("artwork_uri", art[0])
                    qb.add("artwort_caption", art[1])
                    qb.add("artwork_width", art[2])
                    qb.add("artwork_height", art[3])

            res = qb.insert_or_update(cursor, "program_id", compare_query_field="md5", debug=self.debug)
            if self.debug:
                print("Result store/update program %s: %s" % (item["programID"], res))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Schedules Direct JSON Grabber')

    parser.add_argument('--debug',
        action='store_true',
        help='enable logging' )

    args = parser.parse_args()

    cfg = SD_Config(CONFIG_FNM)

    waituntil = cfg.get_int('server', 'waituntil', default=0)
    if waituntil != 0:
        if waituntil > int(time.time()):
            cfg.print_and_log("* wait delay requested until " + int_to_datetime(waituntil))
            cfg.log_close()
            sys.exit(10)

    try:
        user, xpassw = cfg.get_username_xpassword()
        if user == "":
            cfg.print_and_log("* access not configured")
            cfg.print_and_log("* run sdconf.py first")
            cfg.log_close()
            sys.exit(1)

        sd_api = SD_API(config=cfg, debug=(args.debug or cfg.get_debug_active()))
        db = SDDB(sd_api, cfg)

        db.upgrade_database_schema(CURRENT_DATABASE_SCHEMA_VERSION, None)

        token = sd_api.get_token(user, xpassw)
        if token is None:
            cfg.print_and_log("* Error: Token could not be obtained.")
            print("Wrong credentials?")
            cfg.log_close()
            sys.exit(2)

        sd_api.get_status()
        if sd_api.has_expired():
            cfg.print_and_log("* Error: Your account has expired.")
            sys.exit(12)

        cfg.print_and_log("* # of active lineups:", len(sd_api.status["lineups"]))

        ok = sd_api.check_status()
        if not ok:
            cfg.print_and_log("* Abort")
            cfg.cfg['server']['waituntil'] = int(time.time) + 3600
            cfg.write()
            cfg.log_close()
            sys.exit(11)

        lineups_refreshed = db.refresh_lineups_and_stations_if_needed()
        if lineups_refreshed:
            cfg.print_and_log("* changes in lineups")
        unactive_query_stations = db.get_unactive_query_stations()
        db.commit()

        query_stations = db.get_active_query_stations(withname=False)

        cfg.print_and_log("* # of stations to check:", len(query_stations), flush=True)

        if args.debug:
            for station_id in query_stations:
                print(station_id, db.get_station_name_from_station_id(station_id))

        if query_stations != []:
            for query_stations_chunk in chunker(query_stations, MAX_STATIONS_IDS):
                dat = sd_api.get_schedules_md5_max(query_stations_chunk)
                open_schedules, count_invalid_schedules = db.store_schedule_index(dat)

                if cfg.get_debug_active():
                    print("open_schedules", open_schedules)

                if count_invalid_schedules != 0:
                    cfg.printNad_log("* invalid schedules:", count_invalid_schedules, flush=True)

                num_stations_with_new_schedules = len(open_schedules)
                if num_stations_with_new_schedules == 0:
                    cfg.print_and_log("* no changes in schedules", flush=True)
                else:
                    schedule_count = 0
                    for item in open_schedules:
                        schedule_count += len(item)
                    cfg.print_and_log("* %d new schedules in %d stations" % (schedule_count, num_stations_with_new_schedules), flush=True)


                if len(open_schedules) > 0:
                    dat = sd_api.get_schedules(open_schedules)
                    open_programs = db.store_schedules(dat)

                    if cfg.get_debug_active():
                        print("open programs", open_programs)
                    cfg.print_and_log("# unknown programs", len(open_programs), flush=True)

                    if len(open_programs) > 0:

                        # get artwork info for all new/modified programs
                        all_artwork_dat = []
                        open_programs_shorts = sd_api.calc_short_program_ids(open_programs)
                        for chunk in chunker(open_programs_shorts, MAX_ARTWORK_IDS):
                            artwork_dat = sd_api.get_program_artwork_max(chunk)
                            all_artwork_dat.extend(artwork_dat)

                        # calc list of best fitting URLs
                        artwork_info = sd_api.proc_program_artwork_info(all_artwork_dat)

                        # get and store prgram data, inject artwork URL, and store in cache
                        for chunk in chunker(open_programs, MAX_PROGRAM_IDS):
                            dat = sd_api.get_programs_max(chunk)
                            db.store_programs(dat, artwork_info)

        if len(unactive_query_stations) != 0:
            cfg.print_and_log("* Warning: Some stations can not be queried (anymore), because they vanished from the lineups:", flush=True)
            cfg.print_and_log(", ".join(["%s (%s)" % (x[0], x[1])
                                        for x in unactive_query_stations]))

        # only commit program data if they have been downloaded and processed correctly
        db.commit()
        sd_api.close()

        # check for newer version
        if sd_api.check_newer_version_available():
            cfg.print_and_log("* There is a newer version of this software available!")

        cfg.log_flush()

    except Exception as e:
        cfg.log_flush()

        cfg.print_and_log("Version: %s" % __version__)
        cfg.print_and_log("An error has occured: %s" % e)
        f = io.StringIO()
        traceback.print_exc(file=f)
        cfg.print_and_log(f.getvalue(), flush=True)
        cfg.log_close()

        sys.exit(100)

    cfg.log_close()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# (c) 2021 by Michael Strecke
# this program is licensed under the GPLv3, see LICENSE for details

from sdjsongrab import SDDB, SD_Config, CONFIG_FNM
from lib.util import get_current_utc_offset, int_to_date, int_to_utc_datetime, int_to_local_datetime, int_to_xmltv_datetime, min_to_str, xml_escape, date_to_int, tab_and_vertical_splitter
import argparse
import urllib.parse
import datetime
import time
import io
import sys


def cast_splitter(x):
    # split at \t and combine name and cast role (if available)
    return tab_and_vertical_splitter(x, make_single=True)


def crew_splitter(x):
    # split at \ŧ but keep crew name and crew function apart
    res = []
    dat = tab_and_vertical_splitter(x, make_single=False)

    for item in dat:
        if len(item) == 2:
            # map lower case crew function from SD to XMLTV tag
            # (returns None, if not found)
            crew_role_tag = {
                "director": "director",
                "writer": "writer",
                "screenwriter": "writer",
                "executive producer": "producer",
                "producer": "producer"
            }.get(item[1].lower())
            if crew_role_tag is not None:
                res.append((item[0], crew_role_tag))
    return res

class EpgFilter:
    def __init__(self,fnm):
        self.conditions = []

        with open(fnm, "r", encoding="utf8") as fin:
            while True:
                l = fin.readline()
                if l == "":
                    break
                l = l.rstrip()
                if l == "":
                    continue
                if l.startswith("#"):
                    continue
                if l.startswith("genre:"):
                    l = l[6:].strip()
                    self.conditions.append((1, l.lower()))
                else:
                    if l.startswith('-'):
                        self.conditions.append((2, l[1:].strip().lower()))
                    else:
                        self.conditions.append((0, l.lower()))

    def check(self, progdat):
        def pd(key):
            # assert key in progdat, "%s not in %s" % (key, progdat.keys())
            s = progdat[key]
            if s is None:
                return ""
            return s

        for cond_type, cond_info in self.conditions:
            if cond_type == 0:
                s = pd("title120") + "\t" + \
                    pd("episodetitle158") + "\t" + \
                    pd("description100") + "\t" + \
                    pd("description1000")
                if cond_info in s.lower():
                    return True

            elif cond_type == 1:
                if cond_info in pd("genres100").lower():
                    return True

            elif cond_type == 2:
                if cond_info in pd("title120").lower():
                    return False

        return False

class BaseDumper:
    def __init__(self):
        self.out = io.StringIO()

    def _print(self, *args):
        # a print look-a-like
        self.out.write(" ".join([str(x) for x in args]))
        self.out.write("\n")

    def predump(self):
        # anything to be done just before closing the file
        pass

    def dump(self):
        return self.out.getvalue()

    def write(self, filename):
        self.predump()
        if filename is None:
            print(self.dump())
        else:
            open(filename, "w", encoding="utf8").write(self.dump())


class Dumper(BaseDumper):
    """ simple dumper for debug purposes
    """
    def __init__(self):
        BaseDumper.__init__(self)
        self.last_station_id = None
        self.last_startdate_ord = None

        self._print("Time in UTC, UTC Offset: %.1f h" % get_current_utc_offset())

    def add(self, station_id, station_name, startdate_ord, scheddat, progdat):
        if station_id != self.last_station_id:
            self._print("=====================")
            self._print(station_id, station_name)
            self._print()
            self.last_station_id = station_id
            self.last_startdate_ord = None

        if self.last_startdate_ord != startdate_ord:
            self._print()
            self._print(int_to_date(startdate_ord))
            self._print()
            self.last_startdate_ord = startdate_ord
            self._print()

        self._print(int_to_utc_datetime(
            scheddat["airtime"]), min_to_str(scheddat["duration"]))
        self._print("video, audio:",scheddat["videoProperties"], scheddat["audioProperties"])
        self._print("program id:", scheddat["program_id"])
        self._print("show/entity type:",
                    progdat["show_type"], "/", progdat["entity_type"])
        self._print("title:", progdat["title120"])
        self._print("episode title:", progdat["episodetitle158"])
        self._print("desc100:", progdat["description100"])
        self._print("desc1000:", progdat["description1000"])
        self._print("cast1000:", progdat["cast1000"])
        self._print("crew1000:", progdat["crew1000"])
        self._print("artwork:",
              progdat["artwork_width"], progdat["artwork_height"], progdat["artwork_uri"], progdat["artwort_caption"])
        self._print()


class DumperShort(BaseDumper):
    """ simple dumper

    by station and time with short description
    """

    def __init__(self):
        BaseDumper.__init__(self)
        self.last_station_id = None
        self.last_startdate_ord = None
        self.last_airtime_end = None

        self.last_local_date = None

        self._print("local time, UTC Offset: %.1f h" % get_current_utc_offset())

    def add(self, station_id, station_name, startdate_ord, scheddat, progdat):
        if self.last_airtime_end is not None and self.last_airtime_end != scheddat["airtime"]:
            self._print("- " + int_to_local_datetime(self.last_airtime_end,"%H:%M"))

        if station_id != self.last_station_id:
            self._print("=====================")
            self._print("Station: %s (%s)" %
                        (station_name, station_id))
            self.last_station_id = station_id
            self.last_local_date = None

        local_date = int_to_local_datetime(scheddat["airtime"], "%A, %Y-%m-%d")
        if local_date != self.last_local_date:
            self._print()
            self._print(local_date)
            self.last_local_date = local_date
            self._print()

        et = progdat["episodetitle158"]
        if et is None or et == "":
            et = ""
        else:
            et = "\t(" + et + ")"

        self._print(int_to_local_datetime(
            scheddat["airtime"], "%H:%M"), progdat["title120"], et)

        self.last_airtime_end = scheddat["airtime"] + scheddat["duration"]

    def predump(self):
        if self.last_airtime_end is not None:
            self._print("- " + int_to_utc_datetime(self.last_airtime_end,"%H:%M"))

class FilterDumper(DumperShort):
    def __init__(self, filter_fnm):
        DumperShort.__init__(self)
        self.cond = EpgFilter(filter_fnm)

    def add(self, station_id, station_name, startdate_ord, scheddat, progdat):

        if self.cond.check(progdat):
            DumperShort.add(self, station_id, station_name,
                            startdate_ord, scheddat, progdat)

class Xmltv(BaseDumper):
    def __init__(self):
        BaseDumper.__init__(self)
        self.channels = []
        self.programs = []

    def add(self, station_id, station_name, startdate_ord, scheddat, progdat):

        def put(fout, data, key, tag, attrib=None):
            """ store simple entry

            :param fout: output stream
            :type fout: stream
            :param data: dict-like containing the information
            :type data: dict-like
            :param key: key within the dict / or list of key
            :type key: str / list of str
            :param tag: tag name
            :type tag: str
            :param attrib: attribs within opening tag, defaults to None
            :type attrib: str, optional
            """

            # make list of single string
            if type(key) is not list:
                key = [ key ]

            # check each key in turn
            dat = None
            for keyitem in key:
                if keyitem in keyitem:
                    dat = data[keyitem]
                else:
                    dat = None
                if not dat in [None, ""]:
                    break
            if dat in [None, ""]:
                return

            # reformat tag attribute
            if attrib is None:
                attrib = ""
            else:
                attrib = " " + attrib

            fout.write("    <%s%s>%s</%s>\n" % (tag, attrib, xml_escape(dat), tag))

        def put_list(fout, data, key, tag, conv_fun=None):
            """ put a list by inserting a tag multiple times

            :param fout: output stream
            :type fout: stream
            :param data: all data
            :type data: dict-like
            :param key: key within dict
            :type key: str
            :param tag: tag name
            :type tag: str
            :param conv_fun: convert value from value to list, defaults to None
            :type conv_fun: function, optional
            """
            if not key in data.keys():
                return

            dat = data[key]
            if dat in [None, ""]:
                return
            if conv_fun is not None:
                dat = conv_fun(dat)
            if type(dat) is not list:
                dat = [ dat ]

            for item in dat:
                fout.write("    <%s>%s</%s>\n" % (tag, xml_escape(item), tag))

        def put_list2(fout, data, key, conv_fun=None):
            """ put a list , one tag per entry, tag is 2nd item in tuple

            :param fout: output stream
            :type fout: stream
            :param data: all data
            :type data: dict-like
            :param key: key within dict
            :type key: str
            :param conv_fun: convert value from value to list of tuples (value, tag), defaults to None
            :type conv_fun: function, optional
            """
            if not key in data.keys():
                return

            dat = data[key]
            if dat in [None, ""]:
                return

            if conv_fun is not None:
                dat = conv_fun(dat)

            for item in dat:
                if item[1] is not None:
                    fout.write("    <%s>%s</%s>\n" % (item[1], xml_escape(item[0]), item[1]))

        def put_sub(fout, data, key, tag, *, attrib=None, subtag, conv_fun=None):
            """ put tag with sub-tags

            :param fout: output stream
            :type fout: stream
            :param data: base data
            :type data: dict-like
            :param key: key containing the wanted data
            :type key: str
            :param tag: tag name to use in the output
            :type tag: str
            :param subtag: name of subtag repeately used within tag
            :type subtag: str
            :param attrib: attributes for tag, defaults to None
            :type attrib: str, optional
            :param conv_fun: convert value from value to list, defaults to None
            :type conv_fun: function, optional
            """
            if not key in data.keys():
                return

            dat = data[key]
            if dat in [None, ""]:
                return

            if attrib is None:
                attrib = ""
            else:
                attrib = " " + attrib

            if conv_fun is not None:
                dat = conv_fun(dat)

            if type(dat) is not list:
                dat = [ dat ]

            fout.write("    <%s%s>\n" % (tag, attrib))

            for item in dat:
                fout.write("       <%s>%s</%s>\n" % (subtag, xml_escape(item), subtag))
            fout.write("    </%s>\n" % (tag,))

        if not station_name in self.channels:
            self.channels.append(station_name)

        fout = io.StringIO()

        fout.write('  <programme start="%s" stop="%s" channel="%s">\n' % (
                   int_to_xmltv_datetime(scheddat["airtime"]),
                   int_to_xmltv_datetime(scheddat["airtime"] + scheddat["duration"]),
                   station_name)
            )

        put(fout, progdat, "title120", 'title', 'lang="en"')
        put(fout, progdat, "episodetitle158", 'sub-title')
        put(fout, progdat, ["description1000", "description100"], 'desc')
        fout.write("   <credits>\n")
        put_list2(fout, progdat, "crew1000", conv_fun=crew_splitter)
        put_list(fout, progdat, "cast1000", "actor", conv_fun=cast_splitter)
        fout.write("   </credits>\n")
        put_list(fout, progdat, "genres100", "category",
                 conv_fun=tab_and_vertical_splitter)
        put_sub(fout, scheddat, "videoProperties", "video",
                subtag="quality", conv_fun=tab_and_vertical_splitter)
        put_sub(fout, scheddat, "audioProperties", "audio",
                subtag="stereo", conv_fun=tab_and_vertical_splitter)

        put(fout, progdat, "movie_year", "date")
        if progdat["artwork_uri"] is not None:
            ln = ['src="%s"' % urllib.parse.quote(progdat["artwork_uri"])]
            width = progdat["artwork_width"]
            if width is not None:
                ln.append('width="%s"' % width)
            height = progdat["artwork_height"]
            if height is not None:
                ln.append('height="%s"' % height)
            fout.write('    <icon %s />\n' % " ".join(ln))


        fout.write('  </programme>\n')

        self.programs.append(fout.getvalue())

    def dump(self):
        d = int(time.time())
        self._print('<tv date="%s" generator-info-name="sdjson_dumper.py"> ' % d)

        for channel in self.channels:
            self._print("""
  <channel id="%s">
    <display-name lang="en">%s</display-name>
  </channel>
""" % (channel, channel))

        for program in self.programs:
            self._print(program)

        self._print("</tv>")

        return self.out.getvalue()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Exporting xml')

    parser.add_argument('--simple', '-s',
                        action='store_true',
                        help='simple dumper')

    parser.add_argument('--short', '-x',
                        action='store_true',
                        help='dumper with short entries')

    parser.add_argument('--filter', '-f',
                        help='filtered dumper with short entries using this filter file')

    parser.add_argument('--today', '-t',
        action='store_true',
        help='only todays schedule')

    parser.add_argument('--date', '-d',
                        help='output schedules for YYYY-MM-DD')

    parser.add_argument('--output', '-o',
                        help='overwrite standard output name')

    parser.add_argument('station',
        nargs="*",
        help='stationID(s) (leave empty for all stations' )

    args = parser.parse_args()

    cfg = SD_Config(CONFIG_FNM)
    db = SDDB(None, config=cfg)

    today_ord = datetime.date.today().toordinal()
    arg_date = date_to_int(args.date)

    schedule_cond = lambda sched: True  # all
    if arg_date is not None:
        schedule_cond = lambda sched: sched[0] == arg_date
    elif args.today:
        schedule_cond = lambda sched: sched[0] == today_ord
    else:
        schedule_cond = lambda sched: sched[0] >= today_ord

    do_stations = args.station
    if do_stations == []:
        queried_stations = db.get_active_query_stations(withname=True)
        if queried_stations == []:
            print("No stations selected")
            parser.exit(1)

        for s in queried_stations:
            print(s[0], s[1])
            ixs = db.get_schedule_dates_and_indices(s[0])
            filtered_ix = list(filter(schedule_cond, ixs))
            dates = [str(int_to_date(x[0])) for x in filtered_ix]
            print(", ".join(dates))
            do_stations.append(s[0])


    if args.simple:
        dump = Dumper()
        dumpfile = "epg.txt"
    elif args.short:
        dump = DumperShort()
        dumpfile = "epg.txt"
    elif args.filter:
        dump = FilterDumper(args.filter)
        dumpfile = "epg.txt"
    else:
        dump = Xmltv()
        dumpfile = "xmltv.xml"

    if args.output is not None:
        dumpfile = args.output

    # w = db.get_query_stations()
    station_renamer = cfg.get_station_rename()
    sr_orig_stations_with_schedules = set()
    sr_renamed = set()

    for station_id in do_stations:
        orig_station_name = db.get_station_name_from_station_id(station_id)

        station_name = orig_station_name
        if station_renamer is not None:
            newname = station_renamer[0].get(station_id)
            if newname is not None:
                station_name = newname
                sr_renamed.add(newname)


        ixs = db.get_schedule_dates_and_indices(station_id)
        filtered_ix = filter(schedule_cond, ixs)

        filtered_schedules = list(filtered_ix)
        if len(filtered_schedules) != 0:
            sr_orig_stations_with_schedules.add(orig_station_name)
            print(station_name)

        cursor = db.cursor()
        for s_date, s_index in filtered_schedules:
            cursor.execute("SELECT * FROM schedule WHERE scheduleindex=? ORDER BY airtime", (s_index,))
            while True:
                sched = cursor.fetchone()
                if sched is None:
                    break
                prog = db.get_program(sched["program_id"])
                dump.add(station_id, station_name, s_date, sched, prog)

        cursor.close()

    print("* Saving to", dumpfile)
    dump.write(dumpfile)

    if station_renamer is not None:
        if len(sr_renamed) != 0:
            print("* station renamer active")
            print("renamed station:", " ,".join(sr_renamed))

            samenames = sr_orig_stations_with_schedules.intersection(
                station_renamer[1])

            if len(samenames) != 0:
                print('Warning: conflicting station names!!!')
                print("At least one name of a renamed station is identical to a normal station name.")
                print("Both writing schedules under the same station name into the XML file.")
                print("The resulting XML file is invalid.")
                print(" ,".join(samenames))
                sys.exit(10)

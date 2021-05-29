#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import re
import sys

def sdtime_to_unixtime(s: str) -> int:
    """ convert datetime string to timestamp

    :param s: time string (e.g. 2019-01-02T12:56:43Z)
    :return: unix time (integer only)
    """
    if s is None:
        return None
    assert s.endswith('Z')
    s = s[:-1]
    da = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
    return int(da.timestamp())


_date_YMD_co = re.compile(r"(\d\d\d\d)-(\d\d)-(\d\d)")
def date_to_int(daystring: str) -> int:
    """ convert YYYY-MM-DD to int
    """
    if daystring is None:
        return None
    ma = _date_YMD_co.match(daystring)
    if ma is None:
        return None
    d = datetime.date(int(ma.group(1)), int(ma.group(2)), int(ma.group(3)))
    return d.toordinal()


def int_to_date(day_ordinal: int, format="%Y-%m-%d") -> str:
    """ convert ordinal to format (default: YYYY-MM-DD)
    """

    if day_ordinal is None:
        return None
    return datetime.date.fromordinal(day_ordinal).strftime(format)


def int_to_datetime(unixtime: int, format="%Y-%m-%d %H:%M") -> str:
    """ convert unix timestamp to format (default: YYYY-MM-DD HH:MM)
    """
    if unixtime is None:
        return None
    return datetime.datetime.fromtimestamp(unixtime).strftime(format)


def int_to_xmltv_datetime(unixtime: int) -> str:
    """ convert unix timestamp in datetime format used in xmltv xml file (YYYYMMDDHHMMSS +0000)
    """
    if unixtime is None:
        return None
    return datetime.datetime.fromtimestamp(unixtime).strftime("%Y%m%d%H%M%S") + " +0000"


def min_to_str(minutes: int) -> str:
    """ convert minutes to HH:MM
    """
    if minutes is None:
        return None
    h = minutes // 60
    m = minutes % 60
    return "%d:%02d" % (h, m)


def xml_escape(s:str) -> str:
    """ simple xml escaper
    """
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def tab_and_vertical_splitter(basestring, make_single=True):
    """ split string by tab and then by vertical bar

    :param basestring: string containing all elements
    :type basestring: str
    :param make_single: combine bar-split items, defaults to True
    :type make_single: bool, optional
    :return: list of items
    :rtype: list of strings or list of tuples
    """
    dat = basestring.split("\t")

    res = []
    for item in dat:
        y = item.split("|")
        if len(y) > 1:
            if make_single:
                # combine to single String, e.g. "Daniel Radcliffe (Harry Potter)"
                sres = "%s (%s)" % (y[0], y[1])
            else:
                # keep tuple, e.g. ("Tom Magnus", "Director")
                sres = y
        else:
            # no "|" in string -> keep the string
            sres = y[0]
        res.append(sres)
    return res

def chunker(seq, size):
    """ slit sequence into chunks

    :param seq: source sequence
    :type seq: seq or list
    :param size: chunk size
    :type size: int
    :return: list of chunks
    :rtype: generator
    """
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def get_yes_no(allow_none: bool) -> bool:
    while True:
        s = sys.stdin.readline().strip().lower()
        if s == "" and allow_none:
            return None
        if s in ["y", "yes", "1"]:
            return True
        if s in ["n", "no", "0"]:
            return False
        print("Please anweser 'y' or 'n'")

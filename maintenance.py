#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# (c) 2021 by Michael Strecke
# this program is licensed under the GPLv3, see LICENSE for details

from sdjsongrab import SD_Config, CONFIG_FNM, SD_API, SDDB
from lib.util import get_yes_no
import sys
import argparse
import os
import os.path
import urllib.parse

lineupinfodir = "./lineupinfo"


def get_list_entry(sel_list):
    if sel_list is None:
        return None

    maxint = len(sel_list)
    if maxint == 1:
        return sel_list[0]

    cnt = 0
    for item in sel_list:
        cnt += 1
        if type(item) in [list, tuple]:
            s = " - ".join([x for x in item if x is not None])
        else:
            s = item
        print(cnt, s)

    choice = "(1)" if maxint == 1 else "(1-%s)" % maxint
    while True:
        print("Your choice %s:" % choice)
        s = sys.stdin.readline().strip()
        if s == "":
            return None
        try:
            return sel_list[int(s)-1]
        except ValueError:
            print("Not a number")

def handle_program_info(args):
    global db
    cursor = db.cursor()
    if args.subcommand == "listids":
        cursor.execute("SELECT program_id FROM programdata")
        while True:
            res = cursor.fetchone()
            if res is None:
                break
            print(res["program_id"])

    if args.subcommand == "listgenres":
        allgenres = set()
        cursor.execute("SELECT genres100 FROM programdata")
        while True:
            res = cursor.fetchone()
            if res is None:
                break
            g = res["genres100"]
            if g is not None:
                allgenres.update(g.split("\t"))

        glist = sorted(list(allgenres))
        print("Genres:", ", ".join(glist))


    cursor.close()


def handle_stations(args):
    global db
    cursor = db.cursor()

    if args.subcommand in ["query", 'all']:
        where = ""

        if args.subcommand in "query":
            where = " WHERE query_from_sd=1 "

        cursor.execute("SELECT station_id, name FROM stations " + where + "ORDER BY name")
        while True:
            res = cursor.fetchone()
            if res is None:
                break
            print(res["station_id"], res["name"])
    cursor.close()


def lineup_info(args):
    global db

    token = db.sdapi.get_token(user, xpassw)
    if token is None:
        print("Token could not be obtained.")
        print("Wrong credentials?")
        sys.exit(2)

    db.sdapi.get_status()

    os.makedirs(lineupinfodir, exist_ok=True)

    for line in db.sdapi.status["lineups"]:
        outputfnm = os.path.join(lineupinfodir, line["lineup"] + '.txt')
        print("Write info to", outputfnm)

        with open(outputfnm, "w", encoding="utf8") as fout:
            fout.write("Lineup ....: %s\n" % line["lineup"])
            fout.write("Name ......: %s\n" % line["name"])
            fout.write("Last update: %s\n" % line["modified"])

            uri = line["uri"]
            dat = sd_api.api_access("get", uri, use_token=True)

            id_name = {}
            for x in dat["stations"]:
                if type(x) is list:
                    continue
                id_name[x["stationID"]] = x["name"]

            id_map = set( (id_name[x["stationID"]], x["stationID"]) for x in dat["map"])
            id_map = sorted(list(id_map), key=lambda x: x[0].lower())

            fout.write("\nStations: (%s, %s unique)\n\n" % (len(dat["stations"]), len(id_map)))

            for name, stationID in id_map:
                fout.write("%7s %s\n" % (stationID, name))


def get_art(programID):
    global sd_api
    global cfg

    def get_caption(item):
        if item is None:
            return None

        if not "caption" in item:
            return None

        for clist in item["caption"]:
            if "content" in clist:
                return item["caption"]["content"]

        return None

    topdir = cfg.get_str("art", "programs", default="art_programs")

    data = sd_api.get_program_artwork_max([programID])
    if data is None:
        print("* No art found")
        return

    # check through the URIs and get the ones with the highes resulution

    besturl = {}
    for item in data:
        for subitem in item["data"]:
            uri = subitem["uri"]
            try:
                reso = int(subitem["width"]) * int(subitem["height"])
            except ValueError:
                reso = 1

            x = urllib.parse.urlparse(uri)

            if not x.path in besturl:
                _, ext = os.path.splitext(x.path)
                besturl[x.path] = {
                    "pixels": 0,
                    "ext": ext
                }

            if besturl[x.path]["pixels"] < reso:
                besturl[x.path]["pixels"] = reso
                besturl[x.path]["uri"] = uri
                besturl[x.path]["caption"] = get_caption(subitem)

    # downloading images

    cnt = 0
    for key in sorted(list(besturl.keys())):
        print("* downloading", besturl[key]["uri"])
        rdat, rr = sd_api.get_artwork(besturl[key]["uri"])

        if rdat is not None:
            cnt += 1

            ext = besturl[key]["ext"]
            content_type = rr.getheader('Content-Type')
            if content_type == "image/jpeg":
                ext = ".jpg"

            # make cleaned capion part of the filename
            caption = besturl[key].get("caption")
            if caption is None:
                caption = ""
            else:
                caption = " " + caption.replace("//", "_")

            fnm = ("image%02d" % cnt) + caption + ext
            bpath = os.path.join(topdir, programID)
            os.makedirs(bpath, exist_ok=True)
            ppath = os.path.join(bpath, fnm)
            print("* storing as", ppath)
            open(ppath, "wb").write(rdat)

def titlesearch(args):
    search_text = "%" + args.text + "%"
    global db
    cursor = db.cursor()

    title_list = []

    cursor.execute(
        "SELECT program_id, title120, episodetitle158 FROM programdata WHERE title120 LIKE ? OR episodetitle158 LIKE ? ORDER BY title120, episodetitle158", (search_text, search_text))
    while True:
        res = cursor.fetchone()
        if res is None:
            break
        title_list.append(
            (res["program_id"], res["title120"], res["episodetitle158"]))

    print("# hits:", len(title_list))
    if title_list == []:
        return

    x = get_list_entry(title_list)
    if x is not None:
        cursor.execute(
            "SELECT * FROM programdata WHERE program_id=?", (x[0],))
        res = cursor.fetchone()
        print(res["program_id"], res["title120"])
        print(res["description100"])
        print(res["description1000"])
        print("Cast:", ", ".join(res["cast1000"].split("\t")))
        print("Crew:", ", ".join(res["crew1000"].split("\t")))

        print("\nDownload images? (y/N)")
        d = get_yes_no(allow_none=True)
        if d is True:
            get_art(res["program_id"])

    cursor.close()

def database_stats(args):
    global db, cfg
    db.print_database_stats()

    size = os.path.getsize(cfg.get_database_filename())
    print("\nFile size: %.1f MB" % (size / 1048576.0,))

if __name__ == "__main__":
    cfg = SD_Config(CONFIG_FNM)

    user, xpassw = cfg.get_username_xpassword()
    if user == "":
        print("* access not configured")
        print("* run sdconf.py first")
        sys.exit(1)

    sd_api = SD_API(config=cfg, debug=False)
    db = SDDB(sd_api, cfg)

    parser = argparse.ArgumentParser(description='database maintenance')
    subparsers = parser.add_subparsers(title='commands',
                                   description='valid commands')

    parser_a = subparsers.add_parser('programs', help='show program information')
    parser_a.add_argument('subcommand',
        choices=['listids', 'listgenres'],
        help='print infos from program data')
    parser_a.set_defaults(func=handle_program_info)

    parser_b = subparsers.add_parser('stations')
    parser_b.add_argument('subcommand',
                          choices=['all', 'query'],
                          help='print list of all stations, or only stations the grabber will query')
    parser_b.set_defaults(func=handle_stations)

    parser_c = subparsers.add_parser('lineups')
    parser_c.add_argument('subcommand',
                          choices=['info'],
                          help='info about lineups')
    parser_c.set_defaults(func=lineup_info)

    parser_d = subparsers.add_parser('titlesearch')
    parser_d.add_argument('text',
                          help='search for titles with text')
    parser_d.set_defaults(func=titlesearch)

    parser_d = subparsers.add_parser('stats', help='show database stats')
    parser_d.set_defaults(func=database_stats)


    args = parser.parse_args()

    func = getattr(args, "func", None)
    if func is not None:
        args.func(args)
    else:
        parser.print_help()


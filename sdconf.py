#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# (c) 2021 by Michael Strecke
# this program is licensed under the GPLv3, see LICENSE for details

from sdjsongrab import SD_API, SDDB, SD_Config, CONFIG_FNM
import sys
import argparse
import re
from lib.util import get_yes_no

# Error return codes
RC_NO_SERVICES_FOUND = 1
RC_NO_TRANSMITTER_FOUND = 2

def check_credentials(config):
    user, xpass = config.get_username_xpassword()
    if user == "":
        print("* Credentials missing")
        print("Enter your login name:")
        name = sys.stdin.readline().rstrip()
        print("Enter your login password:")
        password = sys.stdin.readline().rstrip()
        xpassword = SD_API.hash_password(password)
        config.set_username_xpassword(name, xpassword)
        config.write()

def read_int(maxint:int) -> int:
    if maxint is None or maxint < 1:
        return None
    choice = "(1)" if maxint == 1 else "(1-%s)" % maxint
    while True:
        print("Your choice %s:" % choice)
        s = sys.stdin.readline().strip()
        if s == "":
            return None
        try:
            return int(s)
        except ValueError:
            print("Not a number")


def show_lineup_change_messages(dat):
    # {'response': 'OK', 'code': 0, 'serverID': '20141201.web.1', 'message': 'Added lineup.', 'changesRemaining': 5, 'datetime': '2021-05-14T20:23:42Z'}
    print("Response:", dat["resonse"])
    print(dat["message"])
    print("Changes remaining:", dat["changesRemaining"])

if __name__ == "__main__":
    cfg = SD_Config(CONFIG_FNM)

    check_credentials(cfg)

    parser = argparse.ArgumentParser(description='Kurzbeschreibung')
    parser.add_argument('--stations',
        action='store_true',
        help='add station to query list' )

    parser.add_argument('--force',
                        action='store_true',
                        help='force a lineup refresh')

    parser.add_argument('--debug',
                        action='store_true',
                        help='print debug messages')

    parser.add_argument('command',
        choices=['station', 'addlineup', 'deletelineup', 'addsinglelineup'],
        nargs='?',
        help='available commands' )

    args = parser.parse_args()

    command = args.command    # may be None
    debug = args.debug
    force_lineup_refresh = args.force

    sd_api = SD_API(config=cfg, debug=debug)
    db = SDDB(sd_api, config=cfg)

    token = None

    lineups_in_db = db.get_lineups_and_modified_date()
    if lineups_in_db == []:
        print("* no lineups in database, forcing lineup refresh")

    if force_lineup_refresh or command in ['addlineup', 'deletelineup', 'addsinglelineup'] or lineups_in_db == []:
        user, xpassw = cfg.get_username_xpassword()
        token = sd_api.get_token(user, xpassw)
        status = sd_api.get_status()
        db.refresh_lineups_and_stations_if_needed()
        db.commit()

    lineups_in_db = db.get_lineups_and_modified_date()
    print("\nYour lineups:")
    cnt = 0
    for lineup in lineups_in_db:
        cnt += 1
        print(cnt, lineup[0])
    print()

    if command == "station":
        query_stations = sorted(db.get_active_query_stations(withname=True), key=lambda x: x[1].lower())

        print("\nActive stations")
        stations = db.get_active_stations()
        for station_id, name in stations:
            if station_id in query_stations:
                tag = "*"
            else:
                tag = " "
            print(tag, station_id, name)

        print("\nStations to query schedule from:")
        for station_id, station_name in query_stations:
            print(station_id, station_name)

        print("\nEnter station ID(s) (leading '-' = remove, e.g. -12345)")
        print("Start line with '?' to search for pattern, e.g. ?xyz:")
        while True:
            inp = sys.stdin.readline().strip()
            if inp == "":
                sys.exit(0)

            if not inp.startswith("?"):
                break

            # string search
            search = inp[1:].strip().lower()

            for station_id, name in stations:
                if search in name.lower():
                    if station_id in query_stations:
                        tag = "*"
                    else:
                        tag = " "
                    print(tag, station_id, name)

        new_ids = [x.strip() for x in inp.split(" ") if x.strip() != ""]
        for new_id in new_ids:
            doquery = True
            if new_id.startswith("-"):
                doquery = False
                new_id = new_id[1:].strip()

            res = db.set_query_station(new_id, do_query=doquery)
            print(res)

        print()
        for station_id, station_name in sorted(db.get_active_query_stations(withname=True), key=lambda x:x[1].lower()):
            print(station_id, station_name)

        db.commit()

    if command == "addlineup":
        dat = sd_api.available_services()

        if dat is None:
            print("No services found")
            sys.exit(RC_NO_SERVICES_FOUND)

        services = [x for x in dat if x["type"] != "LANGUAGES"]
        cnt = 0
        for item in services:
            cnt += 1
            print(cnt, item["type"], item["description"])

        w = read_int(cnt)
        if w is None:
            sys.exit(0)

        serv = services[int(w)-1]

        if serv["type"] == "COUNTRIES":
            dat2 = sd_api.api_access("get", serv["uri"])

            continents = list(dat2.keys())
            print(continents)
            print("\nContinent:")
            print("==========")
            cnt = 0
            for c in continents:
                cnt += 1
                print(cnt, c)
            cont = read_int(cnt)
            if cont is None:
                sys.exit(0)
            continent_key = continents[cont-1]

            countries = dat2[continent_key]

            print("\nCountries:")
            print("==========")
            cnt = 0
            for c in countries:
                cnt += 1
                print(cnt, c["fullName"])
            cou = read_int(cnt)
            if cou is None:
                sys.exit(0)

            country_data = countries[cou-1]
            country_code = country_data["shortName"]

            if country_data.get("onePostalCode", False):
                plz = country_data["postalCodeExample"]
            else:
                plz_regex = country_data["postalCode"]
                if plz_regex.endswith("/gm"):     # bug canadian postal code
                    plz_regex = plz_regex[:-2]

                if plz_regex[0] == "/":
                    plz_regex = "^" + plz_regex[1:]
                if plz_regex[-1] == "/":
                    plz_regex = plz_regex[:-1] + "$"

                plz = None
                while True:
                    print("Enter postal code (example: %s):" %
                        country_data["postalCodeExample"])
                    w = sys.stdin.readline().strip()
                    if w == "":
                        sys.exit(0)
                    ma = re.search(plz_regex, w)
                    if ma is not None:
                        plz = w
                        break

                    print("Postal code does not match", country_data["postalCode"])

            headend_data = sd_api.get_headends(country_code, plz)

            # sort headend data by transport and location
            all_lineups = {}
            for item in headend_data:
                transport = item["transport"]
                location = item["location"]

                if not transport in all_lineups:
                    all_lineups[transport] = {}

                if not location in all_lineups[transport]:
                    all_lineups[transport][location] = []

                all_lineups[transport][location].extend(item["lineups"])

            tkeys = sorted(all_lineups.keys(), key=str.lower)
            sorted_lineups = []
            cnt = 0
            for transport in all_lineups:
                print("\nTransport:", transport)
                lkeys = sorted(all_lineups[transport].keys())
                for location in lkeys:
                    print("  Location:", location)
                    for lu in all_lineups[transport][location]:
                        cnt += 1
                        print("    ", cnt, lu["name"], "->", lu["lineup"])
                        sorted_lineups.append(lu["lineup"])

            x = read_int(len(sorted_lineups))
            if x is None:
                sys.exit(0)

            print("* Requesting lineup", sorted_lineups[x-1])
            dat = sd_api.add_lineup_to_user_account(sorted_lineups[x-1])


            # {'response': 'OK', 'code': 0, 'serverID': '20141201.web.1', 'message': 'Added lineup.', 'changesRemaining': 5, 'datetime': '2021-05-14T20:23:42Z'}
            print("Response:", dat["response"])
            print(dat["message"])
            print("Changes remaining:", dat["changesRemaining"])

        elif serv["type"] == "DVB-S":
            dat2 = sd_api.api_access("get", serv["uri"])

            lineups = [x["lineup"] for x in dat2]
            cnt = 0
            for lineup in lineups:
                cnt += 1
                print(cnt, lineup)

            x = read_int(len(lineups))
            if x is None:
                sys.exit(0)

            print("* Requesting lineup", lineups[x-1])
            dat = sd_api.add_lineup_to_user_account(lineups[x-1])

        elif serv["type"] == "DVB-T":
            print(
                "Enter three letter country code - see: https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3")
            country_code = None
            while True:
                x = sys.stdin.readline().strip().upper()
                if x == "":
                    break
                if len(x) == 3:
                    country_code = x
                    break
                else:
                    print("Code must be three letters long")

            if country_code is None:
                sys.exit(0)

            dat3 = sd_api.get_country_transmitters(country_code)
            if dat3 is None:
                print("no data available")
                sys.exit(RC_NO_TRANSMITTER_FOUND)

            location = sorted(list(dat3.keys()))
            cnt = 0
            for loc in location:
                cnt += 1
                print(cnt, loc, dat3[loc])

            locnum = read_int(len(location))
            if locnum is None:
                sys.exit(0)
            lineup = dat3[location[locnum-1]]
            print("* Requesting lineup", lineup)
            dat = sd_api.add_lineup_to_user_account(lineup)

        else:
            print("Can't handle this type")

    if command == "addsinglelineup":
        print("Enter lineup ID:")
        lineup = sys.stdin.readline().strip()
        if lineup == "":
            sys.exit(0)
        dat = sd_api.add_lineup_to_user_account(lineup)

    if command == "deletelineup":
        dellineup = read_int(len(lineups_in_db))
        dat = sd_api.delete_lineup_from_user_account(
            lineups_in_db[dellineup-1][0])
        unactive_querystations = db.refresh_lineups_and_stations_if_needed()
        if len(unactive_querystations) > 0:
            print("The following stations will query a schedule, but are not part of any lineup:")
            for station_id, name in unactive_querystations:
                print(station_id, name)
            print("Should I deactivate the schedule queries? (y/n)")
            answer = get_yes_no(allow_none=True)
            if answer is True:
                for station_id, name in unactive_querystations:
                    db.set_query_station(station_id, do_query=False)
        db.commit()


# JSON Grabber for SchedulesDirect

Homepage of SchedulesDirect (SD) : https://www.schedulesdirect.org/

# Quick start

## Terms and definitions

An EPG consists of schedules for specifics station.

 * Examples for *stations* are "BBC 2", "CNN", etc.
 * A *schedule* lists at least the start and end times of programs for a specific day.
 * A *program* refers to a single news show, episode of a series, documentation, film, etc.
 * Stations are grouped into *lineups*, usually a list of stations that can be received on a given cable connection, from a given satelite, via antenna on a given location, etc.

## Get an account

 * The first step is to create an account at SchedulesDirect: https://www.schedulesdirect.org/.
 * Note the name and password you're using to log in.
 * Make sure your account is active (trial period or paid subscription).

## Adding lineups

The most difficult part at SD is to figure out which lineups to use (up to 4 lineups are allowed).  Lineups can be managed with `sdconf.py`:

```
./sdconf.py addlineup
```

If it hasn't already done so the programm will query your name and password and store it in the configuration file `sdgrab.ini`.  Only a hashed version of your password will be stored - so your web password is safe.  The hashed version is sufficient to access the EPG information at SD.

You will then be given a choice on how to select a lineup.  Follow the instructions on the screen.  Enter the appropriate number for the choice you want.  And empty input will abort the program.

Note: If you already know the name of the lineup you can add it directly:

```
./sdconf.py addsinglelineup
```

Note: Schedules Direct allows max. 6 "adds" per day.

## Adding (or removing) stations

At this point the database contains the stations you **COULD** query for schedules.  Now you have to configure which stations you actually want to query for schedules.

Enter:

```
./sdconf.py station
```

Again, follow the instructions on the screen.

A sorted list of all stations listed within your lineups will be shown.
Lines starting with "*" show stations that are already added.

Station can be ***added*** by entering their station ID (the number at the start of the line).

You can enter multiple station IDs separated by a space, e.g. 12345 13245 23321

To ***remove*** a station from the list put an "-" in front of the station ID, e.g. -12345.

If you start the line with "?" a filtered station list will be displayed which contains only stations with the text behind the "?". E.g. "?bbc" will display all station with "bbc" in their name.  The search is case insensitive.

## Download the schedules

To download their schedules from SchedulesDirect run:

```
./sdjsongrab.py
```

Messages on the screen show you what's going on or if an error had occured.
The most important messages are also logged to `sdjsongrab.log`.

`sdjsongrab.py` will store the downloaded information in an sqlite database (usually named `sd.db`).

## Generate the EPG

To generate the xmltv EPG file run:

`./geneps.py`

The result will be stored in `xmltv.xml`.


# Important other commands

## Deleting a lineup

```
./sdconf.py deletelineup
```

The list of the currently active lineups is displayed.  Enter the number in front of the line to delete it.

## Create filtered overview lists

`genepg.py` has an option to create an overview list with only specified programs (e.g. of a specific genre of with a specific name in their tile).  For more information see [doc/advanced.md](./doc/advanced.md).
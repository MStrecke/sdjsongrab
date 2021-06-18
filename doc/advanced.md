# Overview

This document describes more rarely used options.

# genepg.py

If you start this program without any parameters it assembles an EPG from the data in its database:

 - It lists all schedules for all marked stations (query stations) for today and the days that follow.
 - The output format is `xmltv`.

All time info is in GMT

## Optional parameters

| parameter | remark |
| --- | --- |
|`-o` or `--output` | writes the output to the given filename. This overwrites the default file name (usually `eps.txt` or `xmltv.xml`). |
| `-t` or `--today` | restict the output to todays schedules |
| `-d YYYY-MM-DD` or `--date YYYY-MM-DD` | restict the output to a specific date |
| `-s` or `--simple` | dumps all information into a simple list.  This is for debug purposes. |
| `-x` or `--short` | concise listing showing start date and the title.  This gives a good overview. |
| `-f filename` or `--filter filename` | searches the EPG for entries matching one of the conditions in the given filename. The output is in the **short** format. |

Please note: ***day*** is defined from 00:00 GMT to 23:59 GMT.  Depending on your timezone this might be in the middle of your day.

## filter

The `filter` functions restricts the output to programs that match the conditions stored in a text file.

The search itself is case-insensitive. The checks are performed in sequence.  The first matching condition determines the outcome.

The output is in the **short** format.

 * normal line: search in the title, episode title and descriptions of an entry
 * line starting with "-": dismiss entry if text is found in the title (only the title is checked)
 * line starting with "genre:": check it this genres has been returned by the schedule

Example of a `filter file`:

```
harry potter
star trek
galactica
-the simpsons
genre: Science fiction
```

This will filter all entries that have "harry potter" OR "star trek" OR "galactica" in the titles or descriptions, AND NOT have "the simpsons" in its title OR have the genre setting "Science fiction".

However, due to the simple compare logic the entry "harry potter and the simpsons" will get through (matches the first line) but a science fiction film with "the simpsons" in its title will be sorted out (matches the fourth line before the genre check is performed).

A list of possible genres (as seen so far) can be obtained using:

```
./maintenance.py programs listgenres
```

A typical calls would be:

```
./genepg.py -f my_filter_file.txt

./genepg.py -f my_filter_file.txt -o favs.txt
```

Without the option `-o` the filtered *short* listing is saved to `eps.txt`.

# maintenance.py

This script contains functions that deal with the database content... mostly.

| command | remark |
| --- | --- |
| `stats` | shows a short overview of the numbers of stations, schedules, and program information stored within the database |
| `stations all` | lists stationID and name of all stations in the database |
| `stations query` | lists stationID and name of all stations whos schedules are queried from Schedules Direct |
| `programs listids` | lists the programIDs of all programs stored in the database |
| `programs genres` | lists all genres used by Schedule Direct so far (can be used in `filter file`) |
| `titlesearch _searchtext_` | searches the database for matching program entries, displays the description, and allows the download of episode art |
| `lineups info` | writes the individual stationIDs of all current lineups to separate files |

Remark: The lineups' stationIDs are useful if you're in the process of selecting lineups.

# The INI file

The INI file stores some configuration options.

```
[access]
username = ... your user name ...
xpassword = ... the hashed login password ...

[database]
filename = sd.db               <- name of the sqlite file with the cached info

[log]
filename = sdjsongrab.log      <- filename of a concise log file, delete entry to disable

[debug]
maindir = debug                <- directory name for extensive debug logs
basefilename = debug.txt       <- filename template of debug file
active = false                 <- true: always active (for sdjsongrab.py)

[art]
programs = program_art         <- top directory name to download episode art

[stationrename]
17155: Channel 4 HD            <- genepg.py: manually rename station
```

## log

If the software runs unattended this will capture if something goes wrong.

Remove this entry to disable this function.

## art

Top directory to store program art.  The images will be store in subdirectories named after the `programID`.

## stationrename

`stationrename` will rename the station name in the `xmltv` file.

The following example shows how it can be used:

A lineup only contains the non-HD version of a station but the device receives the HD version.
In order not to loose the "HD" in the name, this example renames station #17155 ("Channel 4") to "Channel 4 HD".

This is useful if your device renames station according to the names found in the `xmltv` listing (e.g. EyeTV).


## debug

More information on the `debug` option see [programmer.md](./programmer.md)
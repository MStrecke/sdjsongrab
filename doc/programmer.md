# Preface

These utilities were written to get some EPG info back to my EyeTV box after Gracenote downloads became impossible.

Therefore the capabilities of this utility were tailored to display as much information as possible with EyeTV.
However the xmltv file should be compatible with other systems as well.
If you have problems or need some additional data... let me know.

# First steps

The basic steps are described in `README.md`.  Here is a short summary:

 - run `./sdconf.py addlineup`
   - This will guide you through the SchedulesDirect options for selecting a schedule.
 - run `./sdconf.py station`
   - This allows you to select the station that should be queried.
 - run `./sdjsongrab.py`
   - This will download the data from SchedulesDirect.
 - run `./genepg.py`
   - This will generate `xmltv.xml`.


# Retreiving data from Schedules Direct

These are the steps when updateing the schedules from Schedules Direct:

- update database schema if necessary
- get "token"
- get "status"
  - -> lineups
    - check lineups against database
      - on change (other, new, deleted lineups or change in MD5):
        - get new station lists
          - sets all stations to inactive and only the ones in the new lists will become active again
- get schedule index for all station that are active AND have query status (see `sdconf.py`)
  - on new/modified schedules
    - get schedules
      - on new/modified programs
        - get art link
        - get program info


# Simplifications

The amount of data available from Schedules Direct is extensive and `sdjsongrab` only caches a part of it.

 * It tries to select a good image link for the episode (even if EyeTV doesn't displays it).  It does not *download* the images.
 * Schedules Direct also has images of actors.  These are ignored by this program.
 * In the EPG actors and their role are stored as a single string, e.g. "Daniel Radcliffe (Harry Potter)".  The role is usually stored in an xmltv attribute, but EyeTV ignores that attribute and would not display this information otherwise.
 * Only a few "crew" functions (producer etc.) are converted because the number of different functions returned by SD is much larger then those in the xmltv spec and only a fraction of these are actually displayed by EyeTV.


# Troubleshooting

The transactions between `sdjsongrab.py` and SchedulesDirect can be logged.

As most other utilities `sdjsongrab.py` has a `--debug` option.  But you can also activate it more permanently in the `sdjson.ini` file: in the `[debug]` section set `active` to `true`.

 * This will create logfiles in the subdirectory defined in `debug.maindir`.
 * The debug file name is based on `debug.basefilename`.  A timestamp is added.
 * Be aware: These files will be very large.
 * Password and token are redacted.

The debug output activation via the `.ini` file only relates to `sdjsongrab.py`.  However most other utilities in this suite have a `--debug` option as well.


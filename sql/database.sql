PRAGMA user_version=0;

CREATE TABLE lineups (
    id integer primary key,
    lineupname VARCHAR(32),             -- e.g. GBR-1000017-DEFAULT
    name VARCHAR(64),                   -- e.g. Freesat - London
    modified INT,                       -- unix timestamp, change
    uri VARCHAR(128)                    -- access with SD, e.g. /20141201/lineups/GBR-1000017-DEFAULT
);


CREATE TABLE stations (
    id integer primary key,
    station_id varchar(10) NOT NULL,     -- station_id within SD
    name varchar(200),                   -- human readable name of that station
    broadcast_language VARCHAR(10),      -- language, e.g. en or en-GB
    last_modified int,                   -- last modified in this database, unix timestamp
    logo_uri varchar(200),               -- URI of the station logo (not yet implemented)
    logo_width int,                      -- width of station logo
    logo_height int,                     -- heigt of station logo
    logo_md5 VARCHAR(16),                -- md5 of station logo
    logo blob,                           -- picture, if any
    active int default 1 NOT NULL,       -- this station has been seen by a lineup
    query_from_sd int default 0 NOT NULL -- query from SD
);
CREATE INDEX station_id_ix ON stations(station_id);
CREATE INDEX station_query_ix ON stations(query_from_sd);

CREATE TABLE scheduleindex (
    id integer primary key,
    station_id varchar(10),
    startdate int NOT NULL,             -- unix days of schedule
    last_modified int,                  -- unix datestamp
    md5 VARCHAR(16),
    FOREIGN KEY(station_id) REFERENCES stations(station_id)
);
CREATE INDEX schedule_station_id_ix ON scheduleindex(station_id, startdate);

CREATE TABLE schedule (
    id integer primary key,
    station_id varchar(10),       -- information only
    scheduleindex integer,        -- this entry is part of this schedule index
    program_id varchar(20),       -- program_id
    airtime integer,              -- start time in unix time
    duration integer,             -- duration in minutes
    audioProperties varchar(50),  -- free text with those properties
    videoProperties varchar(50),  -- free text with those properties
    FOREIGN KEY(station_id) REFERENCES stations(station_id),
    FOREIGN KEY(scheduleindex) REFERENCES scheduleindex(id)
);
CREATE INDEX schedule_scheduleindex_ix ON schedule(scheduleindex);

CREATE TABLE programdata (
    id integer primary key,
    program_id varchar(20) not null,        -- the program id
    md5 varchar(16) not null,               -- md5 of program server data
    last_access int,                        -- unix time of last access
    title120 varchar(120),                  -- title
    description100 varchar(100),            -- short description
    description1000 varchar(1000),          -- long description
    genres100 varchar(100),                 -- free text, build from server data
    episodetitle158 varchar(158),           -- episode title (150 + "1x03:")
    cast1000 varchar(1000),                 -- free text, build from actor list
    crew1000 varchar(1000),                 -- free text, build from crew list
    entity_type varchar(20),                -- Movie, Episode
    show_type varchar(20),                  -- Series, Feature Film
    movie_year varchar(20),                 -- Movie year
    artwork_uri varchar(200),               -- best matching artwork
    artwork_width int,                      -- width of artwork in pixel
    artwork_height int,                     -- height of artwork in pixel
    artwort_caption varchar(100)            -- caption of artwork (if any)
);
CREATE INDEX program_id_ix ON programdata(program_id);

CREATE TABLE program_artwork (
    id integer primary key,
    program_id varchar(20),                 -- pragram_id, not unique
    uri varchar(200),                       -- uri from which data was downloaded
    aspect varchar(16),                     -- aspect ratio
    caption100 varchar(100),                -- caption
    image BLOB,                             -- the image
    FOREIGN KEY(program_id) REFERENCES programdata(program_id)
);
CREATE INDEX program_art_ix ON program_artwork(program_id);

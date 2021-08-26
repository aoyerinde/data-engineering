import configparser



# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DB_ROLE_ARN = config.get("IAM_ROLE","ARN")
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song_data"
songplay_table_drop = "DROP TABLE IF EXISTS production_songplays"
user_table_drop = "DROP TABLE IF EXISTS production_users"
song_table_drop = "DROP TABLE IF EXISTS production_songs"
artist_table_drop = "DROP TABLE IF EXISTS production_artists"
time_table_drop = "DROP TABLE IF EXISTS production_time"



# CREATE TABLES

staging_events_table_create= ("""
   CREATE TABLE staging_events (
   artist TEXT,
   auth TEXT,
   firstname TEXT,
   gender TEXT,
   iteminsession INT,
   lastname TEXT,
   length NUMERIC,
   level TEXT,
   location TEXT,
   method TEXT,
   page TEXT,
   registration NUMERIC,
   session_id INT,
   song TEXT,
   status INT,
   ts BIGINT,
   useragent TEXT,
   user_id VARCHAR
    )
    diststyle all;
""")


staging_songs_table = ("""
CREATE TABLE staging_song_data (
num_songs INT,
artist_id TEXT,
artist_latitude DOUBLE PRECISION,
artist_longitude DOUBLE PRECISION,
artist_location TEXT,
artist_name TEXT,
song_id TEXT,
title TEXT,
duration DOUBLE PRECISION,
year INTEGER)
    diststyle all;
""")



songplay_table_create = ("""
CREATE TABLE production_songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id  TEXT not null, 
    level  TEXT, 
    song_id  TEXT not null, 
    artist_id  TEXT not null, 
    session_id TEXT , 
    location TEXT, 
    user_agent TEXT,
    foreign key (user_id) references production_users (user_id),
    foreign key (artist_id) references production_artists (artist_id),
    foreign key (song_id) references production_songs (song_id)
    )
    diststyle all;
""")

user_table_create = ("""
CREATE TABLE production_users (
    user_id VARCHAR PRIMARY KEY,
    first_name TEXT,
    last_name TEXT, 
    gender TEXT,
    level TEXT
    )
    diststyle all;
""")

song_table_create = ("""
CREATE TABLE production_songs (   
   song_id TEXT PRIMARY KEY NOT NULL,
   title TEXT,
   artist_id TEXT not null,
   year INTEGER,
   duration DOUBLE PRECISION,
   foreign key (artist_id) references production_artists (artist_id)
       )
   diststyle all;
""")

artist_table_create = ("""
CREATE TABLE production_artists (
    artist_id TEXT PRIMARY KEY NOT NULL,
    name TEXT,
    location TEXT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION
    )
    diststyle all;
""")

time_table_create = ("""
CREATE TABLE production_time(
    start_time TIMESTAMP PRIMARY KEY NOT NULL, 
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month VARCHAR(10),
    year INTEGER, 
    weekday VARCHAR(10)
    )
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from 's3://udacity-dend/log_data' 
access_key_id '{}'
secret_access_key '{}'
region 'us-west-2'
json 'auto'
compupdate off
""").format(KEY,SECRET)

staging_songs_copy = ("""
copy staging_song_data from 's3://udacity-dend/song_data' 
access_key_id '{}'
secret_access_key '{}'
region 'us-west-2'
json 'auto'
compupdate off
""").format(KEY,SECRET)

# FINAL TABLES

songplay_table_insert = ("""

insert into production_songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

SELECT DISTINCT ts,user_id,level,song_id,artist_id,session_id,location,useragent
FROM (
    SELECT timestamp 'epoch' + staged_events.ts/1000 * interval '1 second' as ts, staged_events.user_id, staged_events.level, song_artist.song_id, song_artist.artist_id, staged_events.session_id, staged_events.location, staged_events.useragent

    FROM staging_events staged_events

    JOIN
    (SELECT songs.song_id, artists.artist_id, songs.title, artists.name FROM production_songs songs

    JOIN production_artists artists ON songs.artist_id = artists.artist_id) AS song_artist

    ON (song_artist.title = staged_events.song

    AND song_artist.name = staged_events.artist
    AND AND song_artist.duration = staged_events.length)
        )
    WHERE staged_events.page = 'NextSong'
    )
""")

user_table_insert = ("""
insert into production_users(user_id, first_name,last_name,gender,level)
    
select DISTINCT user_id, 
    firstname,
    lastname,
    gender,
    level from staging_events
    WHERE staging_events.page = 'NextSong';

""")

song_table_insert = ("""
insert into production_songs (song_id, title, artist_id, year, duration)
select DISTINCT song_id, 
    title, 
    artist_id, 
    year, 
    duration from staging_song_data;
""")

artist_table_insert = ("""
insert into production_artists (artist_id, name, location, longitude, latitude) 
select DISTINCT artist_id, 
    artist_name, 
    artist_location,
    artist_longitude,
    artist_latitude
    from staging_song_data;
""")

time_table_insert = ("""
insert into production_time (start_time,hour,day,week,month,year,weekday
)
SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' as start_time, 
DATE_PART(hrs, start_time) as hour,
DATE_PART(day, start_time) as day,
DATE_PART(w, start_time) as week,
DATE_PART(mons,start_time) as month,
DATE_PART(yrs, start_time) as year,
DATE_PART(dow, start_time) as weekday
from songplay_table_insert;
""")

# QUERY LISTS

#create_schema_queries = [staging_schema_create, production_schema_create]

create_table_queries = [staging_events_table_create, staging_songs_table, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]



copy_table_queries = [staging_events_copy, staging_songs_copy]


insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]

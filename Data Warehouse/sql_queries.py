import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_IAM_ARN = config.get("IAM_ROLE", "ARN")
DWH_LOG_DATA = config.get("S3", "LOG_DATA")
DWH_LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
DWH_SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE "staging_events" (
    "artist" TEXT,
    "auth" VARCHAR(40),
    "firstName" TEXT,
    "gender" CHAR(1),
    "itemInSession" INTEGER,
    "lastName" TEXT,
    "length" TEXT,
    "level" TEXT,
    "location" TEXT,
    "method" TEXT,
    "page" TEXT,
    "registration" BIGINT ,
    "sessionId" INTEGER,
    "song" TEXT,
    "status" INTEGER,
    "ts" BIGINT,
    "userAgent" TEXT,
    "userId" INTEGER
); 
""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs" (
    "num_songs" INTEGER,
    "artist_id" VARCHAR(40),
    "artist_latitude" double precision,
    "artist_longitude" double precision,
    "artist_location" TEXT,
    "artist_name" TEXT,
    "song_id" TEXT,
    "title" TEXT,
    "duration" float,
    "year" INTEGER
); 
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    "songplay_id" INT IDENTITY PRIMARY KEY, 
    "start_time" timestamp NOT NULL, 
    user_id int NOT NULL, 
    level varchar NOT NULL, 
    song_id varchar, 
    artist_id varchar, 
    session_id int NOT NULL, 
    location varchar, 
    user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE users(
    user_id int PRIMARY KEY, 
    first_name varchar NOT NULL, 
    last_name varchar NOT NULL, 
    gender char(1) NOT NULL, 
    level varchar NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id varchar PRIMARY KEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int NOT NULL, 
    duration float NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE  artists(
    artist_id varchar PRIMARY KEY, 
    name varchar NOT NULL, 
    location varchar, 
    latitude float, 
    longitude float
)
""")

time_table_create = ("""
CREATE TABLE time(
    start_time timestamp PRIMARY KEY, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday int NOT NULL
) DISTSTYLE ALL;
""")

# STAGING TABLES


staging_events_copy = ("""
COPY staging_events from '{0}'
    credentials 'aws_iam_role={2}'
    region 'us-west-2'
    json '{1}';
""").format(DWH_LOG_DATA, DWH_LOG_JSONPATH, DWH_IAM_ARN)

staging_songs_copy = ("""
COPY staging_songs from '{0}'
    credentials 'aws_iam_role={1}'
    region 'us-west-2'
    json 'auto';
""").format(DWH_SONG_DATA, DWH_IAM_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays ("start_time", user_id, level,  song_id,  artist_id,  session_id,  location , user_agent)
SELECT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second',
userId, level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
FROM staging_events e  
LEFT JOIN staging_songs s ON (s.title = e.song and e.artist = s.artist_name)
where page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users 
SELECT DISTINCT userId, firstName, lastName, gender, level  
from staging_events where page = 'NextSong' 
""")

song_table_insert = ("""
INSERT INTO songs 
SELECT DISTINCT song_id, title, artist_id, year, duration  from staging_songs 
""")

artist_table_insert = ("""
INSERT INTO artists 
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs 
""")

time_table_insert = ("""
INSERT INTO time 
SELECT start_time, 
EXTRACT(hour from start_time), 
EXTRACT(day from start_time), 
EXTRACT(week from start_time), 
EXTRACT(month from start_time), 
EXTRACT(year from start_time), 
EXTRACT(dayofweek from start_time)
FROM songplays 
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]

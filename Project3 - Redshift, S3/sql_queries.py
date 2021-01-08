import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(
                                    artist              VARCHAR,
                                    auth                VARCHAR,
                                    firstName           VARCHAR,
                                    gender              VARCHAR,
                                    itemInSession       VARCHAR,
                                    lastName            VARCHAR,
                                    length              VARCHAR,
                                    level               VARCHAR,
                                    location            VARCHAR,
                                    method              VARCHAR,
                                    page                VARCHAR,
                                    registration        VARCHAR,
                                    sessionId           INTEGER,
                                    song                VARCHAR,
                                    status              INTEGER,
                                    ts                  TIMESTAMP,
                                    userAgent           VARCHAR,
                                    userId              INTEGER);
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
                                    num_songs           INTEGER,
                                    artist_id           VARCHAR,
                                    artist_latitude     FLOAT,
                                    artist_longitude    FLOAT,
                                    artist_location     VARCHAR,
                                    artist_name         VARCHAR,
                                    song_id             VARCHAR,
                                    title               VARCHAR,
                                    duration            FLOAT,
                                    year                INTEGER);
""")

# we use IDENTITY(seed, step) instead of SERIAL in Redshift to create unique row IDs. IDENTITY(0,1) implies start at 0 and increment by 1

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay( 
                                    songplay_id         INTEGER         IDENTITY(0,1)         PRIMARY KEY,
                                    start_time          TIMESTAMP       REFERENCES time(start_time),
                                    user_id             INTEGER         REFERENCES users(user_id),
                                    level               VARCHAR,
                                    song_id             VARCHAR         REFERENCES songs(song_id),
                                    artist_id           VARCHAR         REFERENCES artists(artist_id),
                                    session_id          INTEGER,
                                    location            VARCHAR,
                                    user_agent          VARCHAR);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                    user_id             INTEGER         PRIMARY KEY,
                                    first_name          VARCHAR,
                                    last_name           VARCHAR,
                                    gender              VARCHAR,
                                    level               VARCHAR);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                                    song_id             VARCHAR         PRIMARY KEY,
                                    title               VARCHAR,
                                    artist_id           VARCHAR,
                                    year                INTEGER,
                                    duration            FLOAT);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                    artist_id           VARCHAR         PRIMARY KEY,
                                    name                VARCHAR,
                                    location            VARCHAR,
                                    latitude            FLOAT,
                                    longitude           FLOAT);
""")


time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                    start_time          TIMESTAMP       PRIMARY KEY,
                                    hour                INTEGER,
                                    day                 INTEGER,
                                    week                INTEGER,
                                    month               INTEGER,
                                    year                INTEGER,
                                    weekday             INTEGER);
""")

# STAGING TABLES
staging_events_copy = ("""COPY staging_events FROM '{}'
                          credentials 'aws_iam_role={}'
                          region '{}' 
                          COMPUPDATE OFF STATUPDATE OFF
                          JSON '{}'
                          TIMEFORMAT as 'epochmillisecs'
                          TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
                          """).format(config.get('S3','LOG_DATA'),
                                               config.get('IAM_ROLE', 'ARN'),
                                               config.get('AWS_REGION','REGION'), 
                                               config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs FROM '{}'
                         credentials 'aws_iam_role={}'
                         region '{}' 
                         COMPUPDATE OFF STATUPDATE OFF
                         JSON 'auto'
                         TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
                         """).format(config.get('S3','SONG_DATA'),
                                                config.get('IAM_ROLE', 'ARN'),
                                                config.get('AWS_REGION','REGION'))


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (
                                     start_time,
                                     user_id, 
                                     level, 
                                     song_id, 
                                     artist_id, 
                                     session_id, 
                                     location, 
                                     user_agent
                                     )
                            SELECT DISTINCT to_timestamp(to_char(e.ts, '9999-99-99 99:99:99'),
                                                         'YYYY-MM-DD HH24:MI:SS'),
                                     e.userId, 
                                     e.level, 
                                     s.song_id,
                                     s.artist_id, 
                                     e.sessionid, 
                                     s.artist_location,
                                     e.useragent
                            FROM staging_songs s
                            JOIN staging_events e
                            ON s.title = e.song
                            AND s.artist_name = e.artist
                            AND s.duration = e.length;
""")



user_table_insert = ("""INSERT INTO users(
                                    user_id,
                                    first_name,
                                    last_name,
                                    gender,
                                    level
                                    )
                        SELECT DISTINCT userId,
                                    firstName,
                                    lastName,
                                    gender,
                                    level
                         FROM staging_events
                         WHERE page = 'NextSong'
                         AND userId IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO songs(
                                    song_id,
                                    title,
                                    artist_id,
                                    year,
                                    duration
                                    )
                         SELECT DISTINCT song_id,
                                    title,
                                    artist_id,
                                    year,
                                    duration
                         FROM staging_songs
                         WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists(
                                    artist_id,
                                    name,
                                    location,
                                    latitude,
                                    longitude
                                    )
                          SELECT DISTINCT artist_id,
                                    artist_name,
                                    artist_location,
                                    artist_latitude,
                                    artist_longitude
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO time(
                                    start_time,
                                    hour,
                                    day,
                                    week,
                                    month,
                                    year,
                                    weekday
                                    )
                        SELECT DISTINCT ts, 
                                    EXTRACT(hour from ts), 
                                    EXTRACT(day from ts),
                                    EXTRACT(week from ts), 
                                    EXTRACT(month from ts),
                                    EXTRACT(year from ts), 
                                    EXTRACT(weekday from ts)
                        FROM staging_events
                        WHERE ts IS NOT NULL;


""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

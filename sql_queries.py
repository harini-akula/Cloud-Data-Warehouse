import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist               VARCHAR(255),
        auth                 VARCHAR(255), 
        firstName            VARCHAR(255),
        gender               VARCHAR(10),
        itemInSession        INTEGER,
        lastName             VARCHAR(255),
        length               DECIMAL(12,7),
        level                VARCHAR(10),
        location             VARCHAR(255),
        method               VARCHAR(255),
        page                 VARCHAR(255),
        registration         DECIMAL(16,2),
        sessionId            INTEGER,
        song                 VARCHAR(255)       DISTKEY,
        status               INTEGER,
        ts                   BIGINT,
        userAgent            VARCHAR(255),
        userId               INTEGER
    )
    DISTSTYLE KEY
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        song_id              VARCHAR(255),
        num_songs            INTEGER,
        title                VARCHAR(255)       DISTKEY,
        artist_name          VARCHAR(255),
        artist_latitude      DECIMAL(11,7),
        year                 SMALLINT,
        duration             DECIMAL(11,7),
        artist_id            VARCHAR(255),
        artist_longitude     DECIMAL(11,7),
        artist_location      VARCHAR(255)
    )
    DISTSTYLE KEY
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id          INTEGER        IDENTITY(1,1)    NOT NULL,
        start_time           TIMESTAMP      NOT NULL, 
        user_id              INTEGER, 
        level                VARCHAR(4)    NOT NULL,
        song_id              VARCHAR(50),
        artist_id            VARCHAR(50),
        session_id           INTEGER,
        location             VARCHAR(255)   NOT NULL,
        user_agent           VARCHAR(255) 
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id              INTEGER        NOT NULL,
        first_name           VARCHAR(50)   NOT NULL,
        last_name            VARCHAR(50)   NOT NULL,
        gender               VARCHAR(1)     NOT NULL,
        level                VARCHAR(4)    NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id              VARCHAR(50)   NOT NULL,
        title                VARCHAR(255)   NOT NULL,
        artist_id            VARCHAR(50)   NOT NULL,
        year                 SMALLINT       NOT NULL,
        duration             DECIMAL(11,7)  NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id            VARCHAR(50)   NOT NULL,
        name                 VARCHAR(255)   NOT NULL,
        location             VARCHAR(255),
        latitude             DECIMAL(11,7),
        longitude            DECIMAL(11,7)
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time           TIMESTAMP      NOT NULL, 
        hour                 SMALLINT       NOT NULL, 
        day                  SMALLINT       NOT NULL, 
        week                 SMALLINT       NOT NULL, 
        month                SMALLINT       NOT NULL, 
        year                 SMALLINT       NOT NULL, 
        weekday              SMALLINT       NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {}
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'US-WEST-2'
    JSON {};    
""").format('staging_events', config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY {}
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'US-WEST-2'
    FORMAT AS JSON 'auto';    
""").format('staging_songs', config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second', userId, 
        level, song_id, artist_id, sessionId, location, userAgent
     FROM staging_events e
     LEFT JOIN staging_songs s
     ON e.artist = s.artist_name
     AND e.song = s.title
     AND e.length = s.duration
     WHERE e.page = 'NextSong')
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    WITH users_list AS (SELECT userId, MAX(ts) as mts FROM staging_events GROUP BY userId)
    SELECT 
        e.userId, 
        e.firstName, 
        e.lastName, 
        e.gender, 
        e.level
    FROM staging_events e
    JOIN users_list u
    ON e.userId = u.userId
    AND e.ts = u.mts   
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    (SELECT DISTINCT 
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
        FROM staging_songs 
        WHERE song_id IS NOT NULL)
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    (SELECT DISTINCT 
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude 
        FROM staging_songs 
        WHERE artist_id IS NOT NULL)
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    WITH starttime as (SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS t FROM staging_events)
    SELECT DISTINCT
        t,
        EXTRACT(hr FROM t),
        EXTRACT(d FROM t),
        EXTRACT(w FROM t),
        EXTRACT(mon FROM t),
        EXTRACT(yr FROM t),
        EXTRACT(dow FROM t)
    FROM 
        starttime
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

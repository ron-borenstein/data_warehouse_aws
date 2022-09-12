import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs;"
songplay_table_drop = "DROP TABLE IF EXISTS factSongplay;"
user_table_drop = "DROP TABLE IF EXISTS dimUser;"
song_table_drop = "DROP TABLE IF EXISTS dimSong;"
artist_table_drop = "DROP TABLE IF EXISTS dimArtist;"
time_table_drop = "DROP TABLE IF EXISTS dimTime;"

# CREATE TABLES
    
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS events
    (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender VARCHAR,
        item_in_session INT,
        last_name VARCHAR,
        length DOUBLE PRECISION,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        session_id INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id VARCHAR,
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR,
        artist_name VARCHAR,
        title VARCHAR,
        duration DOUBLE PRECISION,
        year INT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS factSongplay
    (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
        start_time TIMESTAMPTZ NOT NULL REFERENCES dimTime(start_time) sortkey distkey,
        user_id VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        level VARCHAR NOT NULL,
        session_id INT NOT NULL,
        location VARCHAR NOT NULL,
        user_agent VARCHAR NOT NULL
     )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimUser
    (
        user_id INT PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR
    )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimSong
    (
        song_id VARCHAR PRIMARY KEY, 
        title VARCHAR NOT NULL, 
        artist_id VARCHAR NOT NULL sortkey distkey, 
        year INT, 
        duration float NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimArtist
    (
        artist_id VARCHAR PRIMARY KEY sortkey distkey,
        name VARCHAR NOT NULL, 
        location VARCHAR, 
        latitude DOUBLE PRECISION, 
        longitude DOUBLE PRECISION
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimTime
    (
        start_time TIMESTAMPTZ PRIMARY KEY sortkey distkey, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY events FROM {}
    iam_role '{}'
    FORMAT AS json {}
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])



staging_songs_copy = ("""
    COPY songs FROM {}
    iam_role '{}'
    FORMAT AS json 'auto'
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO factSongplay (start_time, user_id, song_id, artist_id, level, session_id, location, user_agent)
    SELECT 
        TIMESTAMP 'epoch' + CAST(ts AS BIGINT)/1000 * INTERVAL '1 second' AS start_time,
        e.user_id,
        s.song_id,
        s.artist_id,
        e.level,
        e.session_id,
        e.location,
        e.user_agent
    FROM events e
    LEFT JOIN songs s ON e.artist = s.artist_name AND e.song = s.title AND e.length = s.duration
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO dimUser (user_id, first_name, last_name, gender, level)
    SELECT 
        DISTINCT user_id,
        first_name,
        last_name,
        gender,
        level
    FROM events
    WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO dimSong (song_id, title, artist_id, year, duration)
    SELECT 
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM songs;
""")

artist_table_insert = ("""
    INSERT INTO dimArtist (artist_id, name, location, latitude, longitude)
    SELECT 
        DISTINCT artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM songs;
""")

time_table_insert = ("""
    INSERT INTO dimTime (start_time, hour, day, week, month, year, weekday)
    SELECT
        DISTINCT TIMESTAMP 'epoch' + CAST(ts AS BIGINT)/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(dow FROM start_time) AS weekday
    FROM events
    WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

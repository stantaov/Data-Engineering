import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ROLE_ARN = config['DWH']['DWH_ROLE_ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_DATA_JSON_PATH = config['S3']['LOG_DATA_JSON_PATH']
SONG_DATA = config['S3']['SONG_DATA']



# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
                                    CREATE TABLE IF NOT EXISTS songplays
                                    (
                                       songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
                                       start_time TIMESTAMP NOT NULL SORTKEY DISTKEY,
                                       user_id INTEGER, 
                                       level CHAR(10), 
                                       song_id VARCHAR, 
                                       artist_id VARCHAR, 
                                       session_id INTEGER, 
                                       location VARCHAR, 
                                       user_agent VARCHAR,
                                       FOREIGN KEY (user_id) REFERENCES users(user_id),
                                       FOREIGN KEY (song_id) REFERENCES songs(song_id),
                                       FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
                                       FOREIGN KEY (start_time) REFERENCES time(start_time)
                                    );
""")


staging_events_table_create= ("""
                                    CREATE TABLE IF NOT EXISTS staging_events
                                    (
                                        artist VARCHAR,
                                        auth VARCHAR,
                                        first_name VARCHAR,
                                        gender CHAR(1),
                                        item_in_session SMALLINT,
                                        last_name VARCHAR,
                                        length DOUBLE PRECISION,
                                        level CHAR(10),
                                        location VARCHAR,
                                        method CHAR(10),
                                        page VARCHAR,
                                        registration VARCHAR,
                                        session_id INTEGER,
                                        song VARCHAR,
                                        status SMALLINT,
                                        ts BIGINT,
                                        user_agent VARCHAR,
                                        user_id INTEGER   
                                    );
                                    
""")

staging_songs_table_create = ("""
                                    CREATE TABLE IF NOT EXISTS staging_songs
                                    (
                                        song_id VARCHAR,
                                        num_songs INTEGER,
                                        title VARCHAR,
                                        artist_name VARCHAR,
                                        artist_latitude FLOAT,
                                        year SMALLINT,
                                        duration DOUBLE PRECISION,
                                        artist_id VARCHAR,
                                        artist_longitude FLOAT,
                                        artist_location VARCHAR
                                    );
""")


user_table_create = ("""
                                    CREATE TABLE IF NOT EXISTS users
                                    (
                                        user_id INTEGER SORTKEY PRIMARY KEY, 
                                        first_name VARCHAR, 
                                        last_name VARCHAR, 
                                        gender CHAR(1), 
                                        level CHAR(10)
                                    );
""")

song_table_create = ("""
                                    CREATE TABLE IF NOT EXISTS songs
                                    (
                                        song_id VARCHAR SORTKEY PRIMARY KEY, 
                                        title VARCHAR, 
                                        artist_id VARCHAR, 
                                        year SMALLINT, 
                                        duration REAL
                                    );
""")

artist_table_create = ("""
                                    CREATE TABLE IF NOT EXISTS artists
                                    (
                                        artist_id VARCHAR SORTKEY PRIMARY KEY, 
                                        artist_name VARCHAR, 
                                        location VARCHAR, 
                                        artist_latitude FLOAT, 
                                        artist_longitude FLOAT
                                    );
""")

time_table_create = ("""
                                    CREATE TABLE IF NOT EXISTS time
                                    (
                                        start_time TIMESTAMP DISTKEY SORTKEY PRIMARY KEY, 
                                        hour INTEGER,
                                        day INTEGER,
                                        week INTEGER,
                                        month INTEGER,
                                        year INTEGER,
                                        weekday INTEGER
                                    );
""")



# STAGING TABLES

staging_events_copy = ("""
                            COPY staging_events
                            FROM {}
                            credentials 'aws_iam_role={}'
                            region 'us-west-2'
                            FORMAT AS json {};
""").format(LOG_DATA, ROLE_ARN, LOG_DATA_JSON_PATH)

staging_songs_copy = ("""
                            COPY staging_songs
                            FROM {}
                            credentials 'aws_iam_role={}'
                            region 'us-west-2'
                            FORMAT AS json 'auto';
""").format(SONG_DATA, ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
                            INSERT INTO songplays(
                            start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent
                            )
                            SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time, se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
                            FROM staging_events AS se
                            JOIN staging_songs AS ss
                            ON se.song = ss.title
                            WHERE se.page = 'NextSong'
                            AND se.session_id NOT IN(
                                SELECT DISTINCT session_id
                                FROM songplays
                            )
                            
""")

user_table_insert = ("""
                        INSERT INTO users(
                        user_id, 
                        first_name, 
                        last_name, 
                        gender, 
                        level
                        )
                        SELECT DISTINCT user_id, first_name, last_name, gender, level
                        FROM staging_events
                        WHERE page = 'NextSong'
                        AND user_id NOT IN(
                            SELECT DISTINCT user_id
                            FROM users
                        )
""")

song_table_insert = ("""
                        INSERT INTO songs(
                        song_id, 
                        title, 
                        artist_id, 
                        year, 
                        duration
                        )
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs
                        WHERE song_id NOT IN (
                                SELECT DISTINCT song_id 
                                FROM songs
                                )  
""")

artist_table_insert = ("""
                            INSERT INTO artists(
                            artist_id, 
                            artist_name, 
                            location, 
                            artist_latitude, 
                            artist_longitude
                            )
                            SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                            FROM staging_songs
                            WHERE artist_id NOT IN (
                                SELECT DISTINCT artist_id 
                                FROM artists
                                )         
""")

time_table_insert = ("""
                        INSERT INTO time(
                        start_time,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday
                        )
                        SELECT start_time, EXTRACT(hour from start_time), EXTRACT(day from start_time), EXTRACT(week from start_time), EXTRACT(year from start_time), EXTRACT(month from start_time), EXTRACT(dayofweek from start_time)
                        FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

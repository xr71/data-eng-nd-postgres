# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

"""
SONG DATA:
    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
    
EVENTS DATA:
    {"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}

    
    songplays:
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    
    users:
        user_id, first_name, last_name, gender, level
          
    songs:
        song_id, title, artist_id, year, duration
    
    artists:
        artist_id, name, location, latitude, longitude
    
    time:
        start_time, hour, day, week, month, year, weekday
    
"""


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
     songplay_id SERIAL PRIMARY KEY,
     start_time date,
     user_id int NOT NULL,
     level varchar,
     song_id varchar NOT NULL,
     artist_id varchar NOT NULL,
     session_id int,
     location varchar,
     user_agent varchar
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
     user_id int PRIMARY KEY,
     first_name varchar NOT NULL,
     last_name varchar NOT NULL,
     gender varchar,
     level varchar
    ) 
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
     song_id varchar PRIMARY KEY, 
     title varchar NOT NULL, 
     artist_id varchar NOT NULL, 
     year int, 
     duration float NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
     artist_id varchar PRIMARY KEY, 
     name varchar NOT NULL, 
     location varchar, 
     latitude float, 
     longitude float
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
     start_time timestamp PRIMARY KEY,
     hour int NOT NULL,
     day int NOT NULL,
     week int NOT NULL,
     month int NOT NULL,
     year int NOT NULL,
     weekday text NOT NULL
    );
""")


#################
# INSERT RECORDS
#################

songplay_table_insert = ("""
    INSERT INTO songplays 
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
    INSERT INTO users 
    (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""
    INSERT INTO songs 
    (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(song_id) DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists 
    (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(artist_id) DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time 
    (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT(start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT distinct song_id, songs.artist_id 
    FROM songs 
    JOIN artists 
        ON songs.artist_id = artists.artist_id 
    WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
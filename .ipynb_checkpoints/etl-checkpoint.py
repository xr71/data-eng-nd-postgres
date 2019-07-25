import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Summary: processes SONG JSON files 
    Description: This function will take the current JSON file passed to it and processes it into Schema ready structure, placing the file into the two dimensional tables Songs and Artists
    
    Parameters: cur=the postgres session cursor, filepath=a directorty path that contains the corresponding JSON files
    Returns: NA
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Summary: processes LOG JSON files 
    Description: This function will take the current JSON file passed to it and processes it into Schema ready structure, placing the file into the two dimensional tables Users and Time.  For the time table, it will parse the entering 'ts' field into a Datetime Python object as well as split out hour, day, week, month, year, and weekday fields. Finally, this function will create a denormalized facts table, Songplays. 
    
    Parameters: cur=the postgres session cursor, filepath=a directorty path that contains the corresponding JSON files
    Returns: NA
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df.page == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit="ms")
    
    # insert time data records
    t = t.apply(lambda x: [x, x.hour, x.day, x.week, x.month, x.year, x.day_name()])
    time_data = list(tuple(t.values))
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')

    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = '', ''

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), 
                     int(row.userId), row.level, songid, artistid, 
                     row.sessionId, row.location, row.userAgent)

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Summary: walks through all files and perform corresponding ETL function
    Description: This function will walk through the filepath passed to it and perform the ETL functions passed to it, which will in turn write the raw files transformed into Postgres schema ready structure via the cursor passed to it and persisting on the Postgres server passed to it via the connection object. 
    
    Parameters: cur=the postgres session cursor, 
        conn=the postgres server connection object,
        filepath=a directorty path that contains the corresponding JSON files,
        func=the function for performing the ETL, in this case, either a function for parsing SONG JSON or LOG JSON
    Returns: NA
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Entry-point function used to connect to Postgres and orchestrate all other functions for loading the JSON raw files to Postgres tables. 
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
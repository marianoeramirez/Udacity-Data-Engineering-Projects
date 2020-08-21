import os
import glob
from io import StringIO

import psycopg2
import pandas as pd
from psycopg2.extras import LoggingConnection
from sql_queries import *


def process_song_file(cur, filepath: str):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(
        df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath: str):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]
    df["datetime"] = pd.to_datetime(df.ts, unit='ms')
    df["hour"] = df.datetime.dt.hour
    df["day"] = df.datetime.dt.day
    df["weekofyear"] = df.datetime.dt.weekofyear
    df["month"] = df.datetime.dt.month
    df["year"] = df.datetime.dt.year
    df["weekday"] = df.datetime.dt.weekday

    # insert time data records
    column_labels = ["ts", "hour", "day", "weekofyear", "month", "year", "weekday"]
    time_df = df[column_labels]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

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
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row["ts"], row["userId"], row["level"], songid, artistid, row["sessionId"], row["location"],
            row["userAgent"],)
        cur.execute(songplay_table_insert, songplay_data)


ARTIST = {}


def process_log_file_copy(cur, filepath: str):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]
    df["datetime"] = pd.to_datetime(df.ts, unit='ms')
    df["hour"] = df.datetime.dt.hour
    df["day"] = df.datetime.dt.day
    df["weekofyear"] = df.datetime.dt.weekofyear
    df["month"] = df.datetime.dt.month
    df["year"] = df.datetime.dt.year
    df["weekday"] = df.datetime.dt.weekday

    # insert time data records
    column_labels = ["ts", "hour", "day", "weekofyear", "month", "year", "weekday"]
    time_df = df[column_labels]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    to_insert = ""

    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        key = (row.song, row.artist, row.length)
        if key in ARTIST:
            songid, artistid = ARTIST[key]
        else:
            cur.execute(song_select, key)
            results = cur.fetchone()

            if results:
                songid, artistid = results
            else:
                songid, artistid = "", ""
            ARTIST[key] = (songid, artistid)

        # insert songplay record
        songplay_data = (
            str(row["ts"]), str(row["userId"]), str(row["level"]), str(songid), str(artistid), str(row["sessionId"]),
            str(row["location"]),
            str(row["userAgent"]),)
        to_insert += "\t".join(songplay_data) + "\n"

    cur.copy_from(StringIO(to_insert), 'songplays', columns=(
        "start_time", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent"))


def process_data(cur, conn, filepath: str, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file_copy)

    conn.close()


if __name__ == "__main__":
    main()

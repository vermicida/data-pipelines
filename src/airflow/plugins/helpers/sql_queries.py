class SqlQueries:

    staging_table_copy = """
                   COPY {}
                   FROM '{}'
            CREDENTIALS 'aws_iam_role={}'
          TIMEFORMAT AS 'epochmillisecs'
                   JSON '{}'
        TRUNCATECOLUMNS
           BLANKSASNULL
            EMPTYASNULL
         COMPUPDATE OFF
    """

    songplays_table_insert = """
        SELECT md5(events.sessionid || events.start_time) songplay_id,
               events.start_time,
               events.userid,
               events.level,
               songs.song_id,
               songs.artist_id,
               events.sessionid,
               events.location,
               events.useragent
          FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                  FROM staging_events
                 WHERE page='NextSong') events
     LEFT JOIN staging_songs songs
            ON events.song = songs.title
           AND events.artist = songs.artist_name
           AND events.length = songs.duration
    """

    users_table_insert = """
        SELECT DISTINCT src.userid,
               src.firstname,
               src.lastname,
               src.gender,
               src.level
          FROM staging_events AS src
         WHERE src.page='NextSong'
    """

    songs_table_insert = """
        SELECT DISTINCT src.song_id,
               src.title,
               src.artist_id,
               src.year,
               src.duration
          FROM staging_songs AS src
    """

    artists_table_insert = """
        SELECT DISTINCT src.artist_id,
               src.artist_name,
               src.artist_location,
               src.artist_latitude,
               src.artist_longitude
          FROM staging_songs AS src
    """

    time_table_insert = """
        SELECT src.start_time,
               EXTRACT(hour FROM src.start_time),
               EXTRACT(day FROM src.start_time),
               EXTRACT(week FROM src.start_time),
               EXTRACT(month FROM src.start_time),
               EXTRACT(year FROM src.start_time),
               EXTRACT(dayofweek FROM src.start_time)
          FROM songplays AS src
    """

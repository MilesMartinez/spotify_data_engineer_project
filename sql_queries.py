# DROP TABLES

artist_table_drop = "DROP TABLE IF EXISTS artist"
album_table_drop = "DROP TABLE IF EXISTS album"
track_table_drop = "DROP TABLE IF EXISTS track"
track_feature_table_drop = "DROP TABLE IF EXISTS track_feature"

# CREATE TABLES

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (
                                artist_id varchar(50),
                                artist_name varchar(255), 
                                external_url varchar(100),
                                genre varchar(100),
                                image_url varchar(100),
                                followers int,
                                popularity int,
                                type varchar(50),
                                artist_uri varchar(100)
                            );
""")

album_table_create = ("""CREATE TABLE IF NOT EXISTS album (
                            album_id varchar(50),
                            album_name varchar(255),
                            external_url varchar(100),
                            image_url varchar(100),
                            release_date date,
                            total_tracks int,
                            type varchar(50),
                            album_uri varchar(100),
                            artist_id varchar(50)
                        );
""")

track_table_create = ("""CREATE TABLE IF NOT EXISTS track (
                            track_id varchar(50),
                            song_name varchar(255),
                            external_url varchar(100),
                            duration_ms int,
                            explicit boolean,
                            disc_number int,
                            type varchar(50),
                            song_uri varchar(100),
                            album_id varchar(50)
                        );
""")

track_feature_table_create = ("""CREATE TABLE IF NOT EXISTS track_feature (
                                track_id varchar(50),
                                danceability double,
                                energy double,
                                instrumentalness double,
                                liveness double,
                                loudness double,
                                speechiness double,
                                tempo double,
                                type varchar(50),
                                valence double,
                                song_uri varchar(100)
                            );
""")

# DROP VIEWS
drop_top_songs_duration_view = "DROP VIEW IF EXISTS vw_top_songs_duration"
drop_top_artists_view = "DROP VIEW IF EXISTS vw_top_artists"
drop_top_songs_tempo_view = "DROP VIEW IF EXISTS vw_top_songs_tempo"
drop_artist_summary_view = "DROP VIEW IF EXISTS vw_song_summary"
drop_album_summary_view = "DROP VIEW IF EXISTS vw_album_summary"

# CREATE VIEWS
create_top_songs_duration_view = ("""CREATE VIEW IF NOT EXISTS vw_top_songs_duration AS
                                        SELECT 
                                            artist_name, 
                                            song_name, 
                                            duration_ms,
                                            rank
                                        FROM
                                        (
                                            SELECT 
                                                artist_name, 
                                                song_name, 
                                                MAX(duration_ms) duration_ms,
                                                RANK() OVER(PARTITION BY artist_name ORDER BY duration_ms DESC) AS rank
                                            FROM artist a 
                                            JOIN album b ON a.artist_id = b.artist_id
                                            JOIN track c ON b.album_id = c.album_id
                                            GROUP BY 1, 2
                                            ORDER BY 1, 3 DESC
                                        ) a
                                        WHERE rank<=10
                                        ORDER BY 1, 3 DESC;    
"""
)
create_top_artists_view = ("""CREATE VIEW IF NOT EXISTS vw_top_artists AS
                                SELECT
                                    artist_id, 
                                    artist_name, 
                                    followers,
                                    RANK() over(ORDER BY followers DESC) as rank 
                                FROM artist
                                ORDER BY 3 DESC
                                LIMIT 20
"""
)
create_top_songs_tempo_view = ("""CREATE VIEW IF NOT EXISTS vw_top_songs_tempo AS
                                        SELECT
                                            artist_name,
                                            song_name,
                                            tempo,
                                            rank
                                        FROM
                                        (
                                            SELECT 
                                                artist_name, 
                                                song_name, 
                                                MAX(tempo) tempo,
                                                RANK() OVER(PARTITION BY artist_name ORDER BY tempo DESC) AS rank
                                            FROM artist a 
                                            JOIN album b ON a.artist_id = b.artist_id
                                            JOIN track c ON b.album_id = c.album_id
                                            JOIN track_feature d ON c.track_id = d.track_id
                                            GROUP BY 1, 2
                                            ORDER BY 1, 3 DESC
                                        ) a
                                        WHERE rank<=10
                                        ORDER BY 1, 3 DESC
"""
)
create_artist_summary_view = ("""CREATE VIEW IF NOT EXISTS vw_artist_summary AS
                                    SELECT
                                        a.artist_id, 
                                        artist_name,
                                        COUNT(distinct album_name) as album_count,
                                        COUNT(distinct song_name) as song_count,
                                        ROUND(AVG(duration_ms),2) avg_song_duration_ms,
                                        ROUND(AVG(tempo),2) avg_tempo,
                                        ROUND(AVG(danceability),2) avg_danceability,
                                        ROUND(AVG(energy),2) avg_energy
                                    FROM artist a 
                                    JOIN album b ON a.artist_id = b.artist_id
                                    JOIN track c ON b.album_id = c.album_id
                                    JOIN track_feature d ON c.track_id = d.track_id
                                    GROUP BY 1, 2 ORDER BY 1;
"""
)
create_album_summary_view = ("""CREATE VIEW IF NOT EXISTS vw_album_summary AS
                                    SELECT
                                        a.artist_id,
                                        b.album_id, 
                                        artist_name,
                                        album_name,
                                        COUNT(distinct song_name) as song_count,
                                        ROUND(AVG(duration_ms),2) avg_song_duration_ms,
                                        ROUND(AVG(tempo),2) avg_tempo,
                                        ROUND(AVG(danceability),2) avg_danceability,
                                        ROUND(AVG(energy),2) avg_energy
                                    FROM artist a 
                                    JOIN album b ON a.artist_id = b.artist_id
                                    JOIN track c ON b.album_id = c.album_id
                                    JOIN track_feature d ON c.track_id = d.track_id
                                    GROUP BY 1, 2, 3, 4 ORDER BY 1;
"""
)

# QUERY LISTS
create_table_queries = [artist_table_create, album_table_create, track_table_create, track_feature_table_create]
drop_table_queries = [artist_table_drop, album_table_drop, track_table_drop, track_feature_table_drop]

create_view_queries = [create_top_songs_duration_view, create_top_artists_view, create_top_songs_tempo_view, create_artist_summary_view, create_album_summary_view]
drop_view_queries = [drop_top_songs_duration_view, drop_top_artists_view, drop_top_songs_tempo_view, drop_artist_summary_view, drop_album_summary_view]


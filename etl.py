import pandas as pd
import re, sqlite3, spotipy
from spotipy.oauth2 import SpotifyClientCredentials

def extract_artist_data(artist_list, spotify):
    """
    Description:
    Retreives artist data via the Spotify API and stores it into a pandas dataframe

    Input:
    - artist_list: list of artist names
    - spotify: authenticated spotipy.client.Spotify object

    Return:
    Pandas DF
    """
    df = pd.DataFrame()
    for artist in artist_list:
        result = spotify.search(artist, limit=1, offset=0, type='artist', market=None)
        data = result['artists']['items']
        temp_df = pd.DataFrame.from_dict(data)
        df = pd.concat([df, temp_df])

    return df

def process_artist_data(df):
    """
    Description:
    Cleans and formats raw artist data from the Spotify API and returns a pandas dataframe

    Input: 
    Pandas DF containing artist data returned by extract_artist_data()

    Return:
    Pandas DF
    """
    artist_df = pd.DataFrame()
    artist_df['artist_id'] = df['id']
    artist_df['artist_name'] = df['name']
    artist_df['external_url'] = df['external_urls'].map(lambda d: d['spotify'])
    artist_df['genre'] = df.genres.map(lambda l: 'no_genre_found' if l==[] else l[0])
    artist_df['image_url'] = df['images'].map(lambda l: l[0]['url'])
    artist_df['followers'] = df['followers'].map(lambda d: d['total'])
    artist_df['popularity'] = df['popularity']
    artist_df['type'] = df['type']
    artist_df['artist_uri'] = df['uri']
    return artist_df

def extract_album_data(artist_id_list, spotify):
    """
    Description:
    Retreives album data via the Spotify API and stores it into a pandas dataframe
    
    Input:
    - artist_id_list: list of spotipy artist ids
    - spotify: authenticated spotipy.client.Spotify object

    Return:
    Pandas DF
    """
    df = pd.DataFrame()
    for id in artist_id_list:
        result = spotify.artist_albums(id, album_type='album', country='US', limit=50, offset=0)
        temp_df = pd.DataFrame.from_records(result['items'])
        df = pd.concat([df, temp_df])

    return df

def process_album_data(df):
    """
    Description:
    Cleans and formats raw album data from the Spotify API and returns a pandas dataframe
    
    Input: 
    Pandas DF containing album data returned by extract_album_data()

    Return:
    Pandas DF
    """
    def clean_date(date):
        if re.match('\d{4}-\d{2}', date, flags=0): #if only year-month is given
            return date+'-01'
        elif re.match('\d{4}', date, flags=0): #if only year is given
            return date+'-01-01'
        else:
            return date

    album_df = pd.DataFrame()
    album_df['album_id'] = df['id']
    album_df['album_name'] = df['name']
    album_df['external_url'] = df['external_urls'].map(lambda d: d['spotify'])
    album_df['image_url'] = df['images'].map(lambda l: l[0]['url'])
    album_df['release_date'] = df['release_date'].map(lambda d: clean_date(d))
    album_df['total_tracks'] = df['total_tracks']
    album_df['type'] = df['type']
    album_df['album_uri'] = df['uri']
    album_df['artist_id'] = df['artists'].map(lambda l: l[0]['id'])
    

    #album_df = album_df.drop_duplicates(subset=['album_name', 'release_date', 'total_tracks', 'artist_id'])

    return album_df

def extract_track_data(album_id_list, spotify):
    """
    Description:
    Retreives track data via the Spotify API and stores it into a pandas dataframe

    Input:
    - album_id_list: list of spotipy album ids
    - spotify: authenticated spotipy.client.Spotify object

    Return:
    Pandas DF
    """
    df = pd.DataFrame()
    for id in album_id_list:
        result = spotify.album_tracks(id, limit=50, offset=0, market='US')
        temp_df = pd.DataFrame.from_records(result['items'])
        temp_df['album_id'] = id
        df = pd.concat([df,temp_df])
    return df

def process_track_data(df):
    """
    Description:
    Cleans and formats raw track data from the Spotify API and returns a pandas dataframe

    Input: 
    Pandas DF containing track data returned by extract_track_data()

    Return:
    Pandas DF
    """
    track_df = pd.DataFrame()
    track_df['track_id'] = df['id']
    track_df['song_name'] = df['name']
    track_df['external_url'] = df['external_urls'].map(lambda d: d['spotify'])
    track_df['duration_ms'] = df['duration_ms']
    track_df['explicit'] = df['explicit'].map(lambda v: False if v=='FALSE' else 'TRUE')
    track_df['disc_number'] = df['disc_number']
    track_df['type'] = df['type']
    track_df['song_uri'] = df['uri']
    track_df['album_id'] = df['album_id']

    return track_df

def extract_track_features(track_id_list, spotify):
    """
    Description:
    Retreives track_features data via the Spotify API and stores it into a pandas dataframe
    
    Input:
    - track_id_list: list of spotipy track ids
    - spotify: authenticated spotipy.client.Spotify object

    Return:
    Pandas DF
    """
    df = pd.DataFrame()
    for id in track_id_list:
        result = spotify.audio_features(tracks=id)
        temp_df = pd.DataFrame.from_records(result)
        df = pd.concat([df, temp_df])
    return df

def process_track_feature(df):
    """
    Description:
    Cleans and formats raw track features data from the Spotify API and returns a pandas dataframe

    Input: 
    Pandas DF containing track features data returned by extract_track_features()

    Return:
    Pandas DF
    """
    track_feature_df = df[['id', 'danceability', 'energy', 'instrumentalness', 'liveness', 'loudness', 'speechiness', 'tempo', 'type', 'valence', 'uri']]
    track_feature_df = track_feature_df.rename(columns={'id':'track_id', 'uri': 'song_uri'})
    return track_feature_df

def load_data(df, table_name):
    """
    Description:
    Loads a given pandas dataframe into a given database table in SQLite
    """
    conn = sqlite3.connect('spotify.db')
    df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
    conn.close()

def main():

    favorite_artists = [
        "Baths",
        "100 gecs",
        "The 1975",
        "My Chemical Romance",
        "Grimes",
        "Aphex Twin",
        "Mitski",
        "Phoebe Bridgers",
        "LCD Soundsystem",
        "Soccer Mommy",
        "Men I Trust",
        "i9bonsai",
        "Turnover",
        "beabadobee",
        "Black Marble",
        "Slow Pulp",
        "Snail Mail",
        "Death Grips",
        "Varsity",
        "Alvvays"
    ]

    spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())

    # Extract
    raw_artist_df = extract_artist_data(favorite_artists, spotify)
    raw_album_df = extract_album_data(raw_artist_df.id.to_list(), spotify)
    raw_track_df = extract_track_data(raw_album_df.id.to_list(), spotify)
    raw_track_feature_df = extract_track_features(raw_track_df.id.to_list(), spotify)
    
    # Transform
    artist_df = process_artist_data(raw_artist_df)
    album_df = process_album_data(raw_album_df)
    track_df = process_track_data(raw_track_df)
    track_feature_df = process_track_feature(raw_track_feature_df)

    # Load
    load_data(artist_df, 'artist')
    load_data(album_df, 'album')
    load_data(track_df, 'track')
    load_data(track_feature_df, 'track_feature')


if __name__ == "__main__":
    main()

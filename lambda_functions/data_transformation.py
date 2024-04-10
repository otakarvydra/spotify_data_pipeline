import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def get_album_dim_data(records):
    album_id_lst = []
    album_name_lst = []
    album_type_lst = []
    album_release_date_lst = []
    album_url_lst = []

    for record in records:
        track = record['track']
        track_album = track['album']
        album_type = track_album['album_type']
        album_id = track_album['id']
        album_name = track_album['name']
        album_release_date = track_album['release_date']
        album_url = track_album['external_urls']['spotify']

        album_id_lst.append(album_id)
        album_name_lst.append(album_name)
        album_type_lst.append(album_type)
        album_release_date_lst.append(album_release_date)
        album_url_lst.append(album_url)

    albums_columns = ['album_id', 'album_name', 'album_type', 'album_release_date', 'album_url']
    albums_values = [album_id_lst, album_name_lst, album_type_lst, album_release_date_lst, album_url_lst]
    albums_df = pd.DataFrame(dict(zip(albums_columns, albums_values)))
    albums_df = albums_df.drop_duplicates()
    return albums_df 

def get_song_dim_data(records):
    song_id_lst = []
    song_name_lst = []
    song_url_lst = []
    song_explicit_lst = []
    song_duration_lst = []

    for record in records:
        track = record['track']
        song_id = track['id']
        song_name = track['name']
        song_url = track['external_urls']['spotify']
        song_explicit = track['explicit']
        song_duration = track['duration_ms']

        song_id_lst.append(song_id)
        song_name_lst.append(song_name)
        song_url_lst.append(song_url)
        song_explicit_lst.append(song_explicit)
        song_duration_lst.append(song_duration)

    songs_columns = ['song_id', 'song_name', 'song_url', 'song_explicit', 'song_duration_ms']
    songs_values = [song_id_lst, song_name_lst, song_url_lst, song_explicit_lst, song_duration_lst]
    songs_df = pd.DataFrame(dict(zip(songs_columns, songs_values)))
    songs_df = songs_df.drop_duplicates()
    return songs_df

def get_artist_dim_data(records):
    artist_id_lst = []
    artist_name_lst = []
    
    for record in records:
        track = record['track']
        artists = track['artists']
        for artist in artists:
            artist_id_lst.append(artist['id'])
            artist_name_lst.append(artist['name'])
    
    artists_columns = ['artist_id', 'artist_name']
    artists_values = [artist_id_lst, artist_name_lst]
    artists_df = pd.DataFrame(dict(zip(artists_columns, artists_values)))
    artists_df = artists_df.drop_duplicates()
    return artists_df

def get_fact_data(records):
    song_id_lst = []
    album_id_lst = []
    artists_id_lst = []
    date_lst = []
    rank_lst = []
    
    for record in records:
        track = record['track']
        album_id = track['album']['id']
        song_id = track['id']
        artists_id_list = []
        for artist in track['artists']:
            artists_id_list.append(artist['id'])
        rank = track['popularity']
        date = record['added_at']
        
        song_id_lst.append(song_id)
        album_id_lst.append(album_id)
        artists_id_lst.append(artists_id_list)
        date_lst.append(date)
        rank_lst.append(rank)
    
    fact_columns = ['song_id', 'album_id', 'artists_list', 'date', 'popularity']
    fact_values = [song_id_lst, album_id_lst, artists_id_lst, date_lst, rank_lst]
    fact_df = pd.DataFrame(dict(zip(fact_columns, fact_values)))
    fact_df['date'] = fact_df['date'].str.split('T').str[0]
    fact_df['artists_list'] = fact_df['artists_list'].astype(str)
    artists_list_lst = fact_df['artists_list'].to_list()
    artists_list_new_lst = []
    for value in artists_list_lst:
        value = value[1:-1]
        value_list = value.split(', ')
        value_list = [i[1:-1] for i in value_list]
        value_list = sorted(value_list)
        value = ','.join(value_list)
        artists_list_new_lst.append(value)
    fact_df['artists_list'] = artists_list_new_lst
    return fact_df
    
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-project-otakar-vydra01'
    Key = 'raw_data/to_process'
    
    spotify_keys = []
    spotify_data = []
    
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_keys.append(file_key)
            spotify_data.append(jsonObject)
    
    for data in spotify_data:
        records = data['items']
        
        albums_df = get_album_dim_data(records)
        artists_df = get_artist_dim_data(records)
        songs_df = get_song_dim_data(records)
        fact_df = get_fact_data(records)
        
        albums_key = 'processed_data/album_data/album_processed_' + str(datetime.now()) + '.csv'
        albums_buffer = StringIO()
        albums_df.to_csv(albums_buffer, index=False)
        albums_content = albums_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=albums_key, Body=albums_content)
        
        artists_key = 'processed_data/artist_data/artist_processed_' + str(datetime.now()) + '.csv'
        artists_buffer = StringIO()
        artists_df.to_csv(artists_buffer, index=False)
        artists_content = artists_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artists_key, Body=artists_content)
        
        songs_key = 'processed_data/song_data/song_processed_' + str(datetime.now()) + '.csv'
        songs_buffer = StringIO()
        songs_df.to_csv(songs_buffer, index=False)
        songs_content = songs_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_key, Body=songs_content)
        
        fact_key = 'processed_data/fact_data/fact_processed_' + str(datetime.now()) + '.csv'
        fact_buffer = StringIO()
        fact_df.to_csv(fact_buffer, index=False)
        fact_content = fact_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=fact_key, Body=fact_content)
        
    s3_resource = boto3.resource('s3')
    
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key' : key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()
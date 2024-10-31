import pandas as pd
import lyricsgenius
import os


def get_lyrics(genius, song_title, artist_name):
    try:
        song = genius.search_song(song_title, artist_name)
        if song:
            return song.lyrics
        else:
            return None
    except Exception as e:
        print(f"Error fetching lyrics for {song_title} by {artist_name}: {e}")
        return None


def scrape():

    # Read the songs_normalize.csv file
    songs = pd.read_csv('data/songs_normalize.csv')

    genius_api = os.environ['GENIUS_API']

    # Initialize the Genius API client
    genius = lyricsgenius.Genius(genius_api)

    # Add a new column for lyrics
    songs['lyrics'] = None

    # Iterate through the songs and fetch the lyrics
    for index, row in songs.iterrows():
        song_title = row['song']
        artist_name = row['artist']
        lyrics = get_lyrics(genius, song_title, artist_name)
        songs.at[index, 'lyrics'] = lyrics

    # Save the updated DataFrame back to the CSV file
    songs.to_csv('songs_normalize_with_lyrics.csv', index=False)
    print('Updated songs with lyrics exported to songs_normalize_with_lyrics.csv')
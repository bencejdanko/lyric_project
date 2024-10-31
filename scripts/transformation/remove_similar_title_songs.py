import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from tqdm import tqdm

# Read the songs_normalize_with_lyrics.csv file
songs = pd.read_csv('songs_normalize_with_lyrics.csv')

# remove songs that with similar titles and artists
def remove_duplicates(songs):

    for index, row in tqdm(songs.iterrows(), total=songs.shape[0], desc=f'Processing songs_normalize_with_lyrics.csv', leave=False):
        artist_songs = songs[songs['artist'] == row['artist']]
        matches = process.extractBests(row['song'], songs['song'], scorer=fuzz.token_sort_ratio, score_cutoff=70)
        for match in matches:
            if match[2] == index:
                continue
            if match[2] in artist_songs.index:
                songs.drop(match[2], inplace=True)
    return songs


if __name__ == '__main__':

    songs = remove_duplicates(songs)

    # Save the updated DataFrame to a new CSV file
    songs.to_csv('songs_normalize_with_lyrics_unique.csv', index=False)

        


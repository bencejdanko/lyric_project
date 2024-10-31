# Use .brotli files from 'chunks/' and compare against 'data/spotify-tracks-genre.csv'
# to identify songs and match with lyrics, then save to 'spotify-tracks-genre-with-lyrics.csv' 

import pandas as pd
import glob
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from tqdm import tqdm
import re

def match():
    # Read the .brotli files from 'chunks/'
    files = glob.glob('chunks/df_chunk_*.parquet.brotli')

    # Define a function to extract the number from the filename
    def extract_number(filename):
        match = re.search(r'df_chunk_(\d+)\.parquet\.brotli', filename)
        return int(match.group(1)) if match else -1

    # Sort the files by the extracted number
    files = sorted(files, key=extract_number)
    files.reverse()

    songs = pd.read_csv('data/spotify-tracks-genre.csv')
    songs['artists'] = songs['artists'].str.split(';')
    songs['artists'] = songs['artists'].apply(lambda x: x if isinstance(x, list) else [])

    # drop mostly instrumental songs
    # songs = songs[songs['instrumentalness'] < 0.5]

    merged_dataframes = []

    for file in tqdm(files, desc='Processing chunks'):
        print(f'Processing {file}')

        chunk = pd.read_parquet(file, engine='pyarrow')
        chunk = chunk[chunk['views'] > 100]
        chunk = chunk[chunk['year'] > 1960]

        for index, row in tqdm(songs.iterrows(), total=songs.shape[0], desc=f'Processing spotify-tracks-genre.csv'):
            if not row['artists']:  # Skip rows where 'artists' list is empty
                continue
            
            artist = row['artists'][0]
            artist_chunk = chunk[chunk['artist'] == artist]
            if artist_chunk.empty:
                continue

            best_match = process.extractOne(row['track_name'], artist_chunk['title'], scorer=fuzz.token_sort_ratio)

            if best_match and best_match[1] > 70: 
                matched_row = pd.DataFrame([row])
                matched_index = artist_chunk[artist_chunk['title'] == best_match[0]].index
                merged_row = pd.merge(matched_row, artist_chunk.loc[matched_index], left_on=['track_name'], right_on=['title'], how='inner')
                merged_dataframes.append(merged_row)
                songs.drop(index, inplace=True)
                chunk.drop(matched_index, inplace=True)

        if merged_dataframes:
            merged_dataframes = pd.concat(merged_dataframes)
            merged_dataframes.to_csv(f'{file}-spotify-tracks-genre-with-lyrics.csv', index=False)
            print(f'Merged songs with lyrics exported to {file}-spotify-tracks-genre-with-lyrics.csv')
            merged_dataframes = []

if __name__ == '__main__':
    match()

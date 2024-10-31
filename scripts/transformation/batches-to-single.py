import glob 
import pandas as pd

# Read the .csv files from 'chunks/'
files = glob.glob('chunks/df_chunk_*.parquet.brotli-spotify-tracks-genre-with-lyrics.csv')

# convert into a single dataframe
merged_dataframes = []
for file in files:
    merged_dataframes.append(pd.read_csv(file))

merged_dataframes = pd.concat(merged_dataframes)
merged_dataframes.to_csv('data/spotify-tracks-genre-with-lyrics.csv', index=False)
import pandas as pd

df = pd.read_csv('data/Spotify_Daily_Streaming.csv')
print(df.head())
print(df.columns)

# drop the track URL column
df = df.drop(columns=['Track URL'])

df.to_csv('data/Spotify_Daily_Streaming_Cleaned.csv', index=False)
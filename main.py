import pandas as pd

df = pd.read_csv('data/spotify-tracks-genre-with-lyrics.csv')
print(df.loc[1])
print(df.iloc[1])
# find row with max instrumentalness
#min_instrumentalness = df['instrumentalness'].idxmax()
#print(df.loc[min_instrumentalness])

import pandas as pd

df = pd.read_csv('data/joined_daily_normalize.csv')
comp_df = pd.read_csv('data/songs_normalize.csv')

# compare their sizes
print('Joined DataFrame size:', df.shape)
print('Normalize DataFrame size:', comp_df.shape)

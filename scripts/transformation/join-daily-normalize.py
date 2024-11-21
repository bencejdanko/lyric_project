import pandas as pd

# Read the normalize_df in full (assuming it's small enough to fit in memory)
normalize_df = pd.read_csv('data/songs_normalize.csv')

# Define the chunk size
chunk_size = 100000  # Adjust based on your memory capacity

# Initialize an empty list to store the results
results = []

# Process the daily_df in chunks
for chunk in pd.read_csv('data/Spotify_Daily_Streaming_Cleaned.csv', chunksize=chunk_size):
    # Perform the join operation
    merged_chunk = pd.merge(chunk, normalize_df, left_on=['Track Name', 'Artist'], right_on=['song', 'artist'], how='inner')
    # Append the result to the list
    results.append(merged_chunk)

# Concatenate all the results into a single DataFrame
final_df = pd.concat(results, ignore_index=True)

# Optionally, save the final DataFrame to a CSV file
final_df.to_csv('data/joined_daily_normalize.csv', index=False)

print('Join operation completed and saved to data/joined_daily_normalize.csv')
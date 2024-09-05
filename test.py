import pandas as pd
import glob
import matplotlib.pyplot as plt
import seaborn as sns

# Get a list of all Parquet files
parquet_files = glob.glob('df_chunk_*.parquet.brotli')

# Define the maximum number of chunks to read
max_chunks = 25  # Adjust this number as needed

# Initialize an empty list to store DataFrames
dataframes = []

# Read up to the specified number of chunks
for i, file in enumerate(parquet_files):
    if i >= max_chunks:
        break
    dataframes.append(pd.read_parquet(file))

# Concatenate all DataFrames into a single DataFrame
df = pd.concat(dataframes, ignore_index=True)

# Context for columns:

# dtype = {
#     'title': 'string',
#     'tag': 'string',
#     'artist': 'string',
#     'year': 'int16',
#     'views': 'int32',
#     'lyrics': 'string'
# }


# Example visual analysis: Distribution of views
plt.figure(figsize=(10, 6))
sns.histplot(df[df['year'].between(1950, 2024)]['year'], bins=50, kde=True)
plt.title('Distribution of Views')
plt.xlabel('Year')
plt.ylabel('Frequency')
plt.savefig('distribution_of_years.png')
plt.close()
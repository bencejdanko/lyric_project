import pandas as pd

# Define data types for each column
dtype = {
    'title': 'string[pyarrow]',
    'tag': 'string[pyarrow]',
    'artist': 'string[pyarrow]',
    'year': 'int16',
    'views': 'int32',
    'lyrics': 'string[pyarrow]'
}

# Define the chunk size
chunk_size = 100000  # Adjust based on your memory capacity

# Initialize the chunk counter
chunk_counter = 0

# Process the CSV file in chunks
for chunk in pd.read_csv('df_eng.csv', dtype=dtype, chunksize=chunk_size, low_memory=False, memory_map=True):
    print(f'Processing chunk {chunk_counter}')
    chunk.to_parquet(
        f'df_chunk_{chunk_counter}.parquet.brotli', 
        compression='brotli',
        index=False,
        engine='pyarrow'
    )
    chunk_counter += 1

print('All chunks processed and converted to parquet')
import pandas as pd

## filePath:
df = pd.read_csv('./moviesList.csv')

### movie id filtering:
duplicate_ids = df[df.duplicated(subset=['id'], keep=False)]
if not duplicate_ids.empty:
    print("Duplicate IDs found in the CSV data. Removing duplicates...")
    df.drop_duplicates(subset=['id'], keep='first', inplace=True)
    df.to_csv('./filteredMoviesList.csv', index=False)
    print("Duplicates removed")
else:
    print("No duplicate IDs found in the CSV data.")

### genre id filtering: 
print("Updating genre_ids....")
df['genre_ids'] = df['genre_ids'].str.replace('[', '').str.replace(']', '').str.split(',').str[0]
df['genre_ids'] = df['genre_ids'].astype(str).replace('', '10770')
df['genre_ids'] = df['genre_ids'].astype(int)
df.to_csv('./filteredMoviesList.csv', index=False)
print('Updated genre_ids!')

### Title filtering with 'S' and 'H':
print("Filtering the titles....")
filtered_df = df[df['title'].str.startswith(('S', 'H'))]
filtered_df.to_csv('./filteredMoviesList.csv', index=False)
print("Filtered the titles!")
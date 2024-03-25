import pandas as pd


df = pd.read_csv('./moviesList.csv')

duplicate_ids = df[df.duplicated(subset=['id'], keep=False)]

if not duplicate_ids.empty:
    print("Duplicate IDs found in the CSV data. Removing duplicates...")
    df.drop_duplicates(subset=['id'], keep='first', inplace=True)
    df.to_csv('./filteredMoviesList.csv', index=False)
    print("Duplicates removed")
else:
    print("No duplicate IDs found in the CSV data.")
import pandas as pd
from pathlib import Path
import json
import numpy as np


base_dir = Path(__file__).resolve().parent.parent.parent.parent
raw_movies_path = base_dir / 'popular_movies.json'

with open(raw_movies_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Wrap objects in an array and add commas between objects
corrected_json = '[\n' + content.replace('}\n{', '},\n{') + '\n]'

# Save corrected JSON
# corrected_path = base_dir / 'corrected_movies.json'
# with open(corrected_path, 'w', encoding='utf-8') as f:
#     f.write(corrected_json)

# Load into DataFrame
data = json.loads(corrected_json)
df = pd.DataFrame(data)


# identify if any columns have missing data
missing_values = df.isnull().sum()
print("Missing Values:\n", missing_values)


# Convert genre_ids from list to tuple
df['genre_ids'] = df['genre_ids'].apply(lambda x: tuple(x) if isinstance(x, list) else x)

# check and process for duplicates
duplicates = df.duplicated().sum()
print(f"Number of duplicate rows: {duplicates}")

df.drop_duplicates(inplace=True)

# Check and process for Invalid Dates
df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
invalid_dates = df[df['release_date'] > pd.Timestamp.today()]
print("Movies with future release dates:\n", invalid_dates)

df = df[df['release_date'] <= pd.Timestamp.today() + pd.DateOffset(years=10)]

popularity_outliers = df[(np.abs(df['popularity'] - df['popularity'].mean()) > (5 * df['popularity'].std()))]
print("Popularity Outliers:\n", popularity_outliers)
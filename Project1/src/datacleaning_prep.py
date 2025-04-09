import pandas as pd
import ast

# Load data
def load_data(file_path):
    return pd.read_csv(file_path).head()

# Drop irrelevant columns
def drop_columns(columns):
    return df.drop(columns=['adult', 'imdb_id', 'original_title', 'video', 'homepage'])

# Evaluate JSON-like columns
def evaluate_json_column(column):
    try:
        return ast.literal_eval(column) if pd.notna(column) else {}
    except (ValueError, SyntaxError):
        return {}
    
## check this
def parse_json_columns(df, json_columns):
    for col in json_columns:
        df[col] = df[col].apply(evaluate_json_column)
    return df

# Extract collection name
def extract_collection_name(value):
    if pd.notnull(value) and isinstance(value, dict):
        return value.get('name')
    return None

# Break JSON list into string

#check this 
def break_data_points(df, init_column, new_column):
    df[new_column] = df[init_column].apply(lambda x: ' | '.join(d['name'] for d in x) if isinstance(x, list) else None)
    return df

# Cast and crew processing
def extract_cast_names(credits):
    return [member['name'] for member in credits.get('cast', [])]

def extract_crew_names(credits):
    return [member['name'] for member in credits.get('crew', [])]

def extract_director(credits):
    for member in credits.get('crew', []):
        if member.get('job') == 'Director':
            return member.get('name')
    return None

#check this
def add_cast_crew_director(df):
    df['cast'] = df['credits'].apply(lambda x: ' | '.join(extract_cast_names(x)))
    df['crew'] = df['credits'].apply(lambda x: ' | '.join(extract_crew_names(x)))
    df['director'] = df['credits'].apply(extract_director)
    df['cast_size'] = df['credits'].apply(lambda x: len(x.get('cast', [])))
    df['crew_size'] = df['credits'].apply(lambda x: len(x.get('crew', [])))
    return df

# Value counts

def get_value_counts(df, column):
    return df[column].value_counts()
    
def normalize_anomalies(genre_string):
    genres = [g.strip() for g in genre_string.split('|')]
    return ' | '.join(sorted(genres))

# Convert to numeric and datetime
def convert_to_numeric(df, column):
    df[column] = pd.to_numeric(df[column], errors='coerce')
    return df[column].info()



def convert_to_datetime(df, column):
    df[column] = pd.to_datetime(df[column])
    return df[column].info()


# Check for zeroes and missing

def check_zero_in_column(df, column):
    if (df[column] == 0).any():
        print(f"Column '{column}' contains at least one value equal to 0.")
    else:
        print(f"No zero values found in column '{column}'.")

def vote_count_zero(df, column):
    return df[df[column] == 0]

def check_for_nodata(df, column):
    return df[df[column] == 0]

# Handle released movies
def released_movie(df, status_column):
    df_new = df[df[status_column] == 'Released']
    df.drop(columns=[status_column], inplace=True)
    return df_new

# Finalize and save cleaned data
def reorder_and_save(df, path):
    new_order = ['id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
                 'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
                 'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
                 'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size']
    reordered_df = df[new_order + [col for col in df.columns if col not in new_order]]
    reordered_df.reset_index(drop=True, inplace=True)
    reordered_df.to_csv(path, index=False)
    return reordered_df.head()
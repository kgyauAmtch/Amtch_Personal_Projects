import ast
from pyspark.sql import functions as dc

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Evaluate JSON-like columns
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType




from pyspark.sql.types import StructType, ArrayType, StringType

def get_schemas():
    schemas = {
        "collection_schema": StructType()
            .add("id", StringType())
            .add("name", StringType())
            .add("poster_path", StringType())
            .add("backdrop_path", StringType()),

        "genre_schema": ArrayType(
            StructType().add("id", StringType()).add("name", StringType())
        ),

        "lang_schema": ArrayType(
            StructType()
            .add("english_name", StringType())
            .add("iso_639_1", StringType())
            .add("name", StringType())
        ),

        "company_schema": ArrayType(
            StructType()
            .add("id", StringType())
            .add("logo_path", StringType())
            .add("name", StringType())
            .add("origin_country", StringType())
        ),

        "country_schema": ArrayType(
            StructType()
            .add("iso_3166_1", StringType())
            .add("name", StringType())
        )
    }
    return schemas

from pyspark.sql.functions import from_json, col, regexp_replace, udf
from pyspark.sql.types import StringType

def fix_json_format(df, column):
    return df.withColumn(column, regexp_replace(column, "'", '"'))

def parse_json_columns(df):
    schemas = get_schemas()
    for column in ["belongs_to_collection", "genres", "spoken_languages", "production_companies", "production_countries"]:
        df = fix_json_format(df, column)

    return df \
        .withColumn("belongs_to_collection", from_json("belongs_to_collection", schemas["collection_schema"])) \
        .withColumn("genres", from_json("genres", schemas["genre_schema"])) \
        .withColumn("spoken_languages", from_json("spoken_languages", schemas["lang_schema"])) \
        .withColumn("production_companies", from_json("production_companies", schemas["company_schema"])) \
        .withColumn("production_countries", from_json("production_countries", schemas["country_schema"]))



def join_names(arr):
    if arr:
        return "|".join([x['name'] for x in arr if x.get('name')])
    return None

join_names_udf = udf(join_names, StringType())

def extract_and_clean_columns(df):
    return df \
        .withColumn("collection_name", col("belongs_to_collection.name")) \
        .withColumn("Genre names", join_names_udf("genres")) \
        .withColumn("Spoken languages", join_names_udf("spoken_languages")) \
        .withColumn("Production companies", join_names_udf("production_companies")) \
        .withColumn("Production countries", join_names_udf("production_countries"))



def drop_irrelevant_columns(df):
    cols_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    return df.drop(*cols_to_drop)

def inspect_column_counts(df, column_name):
    df.select(column_name).groupBy(column_name).count().orderBy("count", ascending=False).show(truncate=False)









# ----------------------------------------

# def parse_json_columns(df: DataFrame, columns: dict) -> DataFrame:
#     """
#     Parses multiple JSON string columns into StructType columns based on provided schemas.

#     Args:
#         df (DataFrame): The input Spark DataFrame.
#         columns (dict): A dictionary where keys are column names (str) and values are their respective StructType schemas.
#     Returns:
#         DataFrame: The DataFrame with additional StructType columns parsed from JSON strings.
#     """
#     df_parsed = df
#     for col_name, schema in columns.items():
#         df_parsed = df_parsed.withColumn(
#             f"{col_name}_struct",
#             from_json(col(col_name), schema)
#         )
#     return df_parsed

# Evaluate JSON-like columns
def evaluate_json_column(column):
    try:
        return ast.literal_eval(column) if pd.notna(column) else {}
    except (ValueError, SyntaxError):
        return {}
    
def convert_json_columns(df, json_columns):
    for col in json_columns:
        df[col] = df[col].apply(evaluate_json_column)
    return df

# ## check this
# def parse_json_columns(df, json_columns):
#     for col in json_columns:
#         df[col] = df[col].apply(evaluate_json_column)
#     return df.head()

# Extract collection name
def extract_collection_name(value):
    if pd.notnull(value) and isinstance(value, dict):
        return value.get('name')
    return None

# Break JSON list into string

#check this 
def break_data_points(df, init_column, new_column):
    df[new_column] = df[init_column].apply(lambda x: ' | '.join(d['name'] for d in x) if isinstance(x, list) else None)
    return df[new_column]

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
    return df.head()

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
def released_movies(df):
    df = df[df['status'] == 'Released'].drop(columns=['status'])
    return df.head()



# Finalize and save cleaned data
def reorder_and_save(df,new_order,path):
    # new_order = ['id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
    #              'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
    #              'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
    #              'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size']
    reordered_df = df[new_order + [col for col in df.columns if col not in new_order]]
    reordered_df.reset_index(drop=True, inplace=True)
    reordered_df.to_csv(path, index=False)
    return reordered_df.head()

# def franchisedf_save(df, path):
#     franchise_summary = Franchise_df.agg(
#     movie_count=('id', 'count'),
#     total_budget=('budget_musd', 'sum'),
#     mean_budget=('budget_musd', 'mean'),
#     total_revenue=('revenue_musd', 'sum'),
#     mean_revenue=('revenue_musd', 'mean'),
#     mean_rating=('vote_average', 'mean')).reset_index()
#     franchise_summary.reset_index(drop=True, inplace=True)
#     franchise_summary.to_csv(path, index=False)
#     return franchise_summary.head()
import ast
from pyspark.sql import functions as dc
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, to_date 
from pyspark.sql.types import *

from pyspark.sql import functions as F

def extract_and_clean_json_columns(df):
    """
    Extract and clean nested JSON-like columns from the movie dataset.

    Args:
        df (DataFrame): Input Spark DataFrame with TMDB movie data.

    Returns:
        DataFrame: Cleaned DataFrame with flattened fields.
    """
    
    # Extract nested fields
    df = df.withColumn("collection_name", F.col("belongs_to_collection.name"))
    df = df.withColumn("genre_names", F.expr("concat_ws('|', transform(genres, x -> x.name))"))
    df = df.withColumn("spoken_languages", F.expr("concat_ws('|', transform(spoken_languages, x -> x.name))"))
    df = df.withColumn("production_countries", F.expr("concat_ws('|', transform(production_countries, x -> x.name))"))
    df = df.withColumn("production_companies", F.expr("concat_ws('|', transform(production_companies, x -> x.name))"))


    return df

def value_counts(df, col_name, limit=20):
    """
    Mimics Pandas' value_counts() in PySpark.

    Args:
        df (DataFrame): Spark DataFrame.
        col_name (str): Column name to count values for.
        limit (int): Number of top results to return.

    Returns:
        DataFrame: A DataFrame showing unique values and their counts.
    """
    return df.groupBy(col_name).count().orderBy("count", ascending=False).limit(limit)
# from pyspark.sql import DataFrame
# from pyspark.sql.functions import col, to_date
# from pyspark.sql.types import DoubleType, IntegerType

def convert_column_types(df):
    """
    Converts data types for selected columns:
    - 'budget', 'id', 'popularity' to numeric (invalid entries become null)
    - 'release_date' to date format
    
    Args:
        df (DataFrame): Input Spark DataFrame
    
    Returns:
        DataFrame: Transformed DataFrame with updated column types
    """
    df = df.withColumn("budget", col("budget").cast(DoubleType())) \
           .withColumn("id", col("id").cast(IntegerType())) \
           .withColumn("popularity", col("popularity").cast(DoubleType())) \
           .withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))
    df.printSchema()
    return df


from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def replace_unrealistic_data(df):
    """
    Clean and adjust movie data:
    - Replace 0 in 'budget', 'revenue', 'runtime' with null (NaN).
    - Convert 'budget' and 'revenue' to million USD.
    - For movies with 'vote_count' == 0, adjust 'vote_average' accordingly.
    
    Args:
        df (DataFrame): Input Spark DataFrame
    
    Returns:
        DataFrame: Cleaned and adjusted DataFrame
    """
    # Replace 0 with null for 'budget', 'revenue', and 'runtime'
    df = df.withColumn("budget", F.when(F.col("budget") == 0, None).otherwise(F.col("budget"))) \
           .withColumn("revenue", F.when(F.col("revenue") == 0, None).otherwise(F.col("revenue"))) \
           .withColumn("runtime", F.when(F.col("runtime") == 0, None).otherwise(F.col("runtime")))

    # Convert 'budget' and 'revenue' to million USD
    df = df.withColumn("budget_musd", F.col("budget") / 1000000) \
           .withColumn("revenue_musd", F.col("revenue") / 100000)

    # For movies with vote_count == 0, adjust 'vote_average' accordingly (e.g., set to null or a default value)
    df = df.withColumn("vote_average", F.when(F.col("vote_count") == 0, None).otherwise(F.col("vote_average")))

    df= df.withColumn( "overview",F.when(F.col("overview") == "No Data", None).otherwise(F.col("overview")))\
          .withColumn("tagline", F.when(F.col("tagline") == "No Data", None).otherwise(F.col("tagline")))
          
    return df


def clean_duplicates_and_missing_data(df):
    """
    Removes duplicates and drops rows with missing 'id' or 'title'.
    Handles 'MapType' columns by dropping them temporarily during deduplication.
    
    Parameters:
    df (DataFrame): The input PySpark DataFrame.
    
    Returns:
    DataFrame: Cleaned DataFrame.
    """
    # Identify MapType columns
    map_columns = [col for col, dtype in df.dtypes if dtype.startswith('map')]
    
    # Temporarily drop MapType columns for deduplication
    df_no_map = df.drop(*map_columns)
    
    # Remove duplicates
    df_no_map = df_no_map.distinct()
    
    # Drop rows with null 'id' or 'title'
    df_no_map = df_no_map.filter(F.col('id').isNotNull() & F.col('title').isNotNull())
    
    # Ensure 'id' is included in the right-side DataFrame for the join
    map_columns_with_id = ['id'] + map_columns  # Add 'id' to map columns

    # Get the map columns along with 'id' for the join
    df_map_columns = df.select(*map_columns_with_id)

    # Perform the join
    df_cleaned = df_no_map.join(df_map_columns, on=['id'], how='left')
    
    return df_cleaned


def filter_non_null(df, min_non_null_cols=10):
    """
    Filters DataFrame to:
    1. Keep rows with at least `min_non_null_cols` non-null values.
    2. Keep only movies where status == 'Released'.
    3. Drop the 'status' column after filtering.
    
    Parameters:
    df (DataFrame): Input DataFrame.
    min_non_null_cols (int): Minimum number of non-null values required per row.
    
    Returns:
    DataFrame: Filtered and cleaned DataFrame.
    """
    # Count number of non-null values per row
    non_null_expr = sum(col(c).isNotNull().cast("int") for c in df.columns)
    
    df = df.withColumn("non_null_count", non_null_expr) \
                    .filter(col("non_null_count") >= min_non_null_cols) \
                    .drop("non_null_count")

    #
    
    return df


def released_movies(df):
    #  Filter only released movies and drop 'status'
    df = df.filter(col("status") == "Released").drop("status")
    return df


from pyspark.sql.functions import col, size, expr, filter, explode, when
from pyspark.sql.types import StringType, ArrayType, StructType

def extract_credits_info(df):
    """
    Extracts cast, cast_size, director(s), and crew_size from nested credits column.
    
    Args:
        df (DataFrame): PySpark DataFrame with a 'credits' Struct column.
    
    Returns:
        DataFrame: Transformed DataFrame with extracted fields.
    """
    df = df.withColumn("cast", col("credits.cast")) \
           .withColumn("crew", col("credits.crew")) \
           .withColumn("cast_size", size(col("cast"))) \
           .withColumn("crew_size", size(col("crew"))) \
           .withColumn("director", expr("""filter(crew, x -> x.job = 'Director')""")) \
           .withColumn("director", expr("""transform(director, x -> x.name)""")) \
           .withColumn("director", expr("""array_join(director, '|')"""))
    df=df.drop('crew')
    return df

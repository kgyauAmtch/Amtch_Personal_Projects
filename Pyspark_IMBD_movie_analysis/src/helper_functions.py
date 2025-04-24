import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *



def schema_build():
    """
    Define the schema for the  DataFrame based on TMDB API response structure.
    
    Returns:
        StructType: Spark schema for movie data
    """
    basic_fields = [
        StructField('id', IntegerType(), False),  # Movie ID (non-nullable integer)
        StructField('title', StringType(), True),  # Movie title (string, nullable)
        StructField('tagline', StringType(), True),  # Movie tagline (string, nullable)
        StructField('release_date', StringType(), True),  # Release date (string, e.g., "2019-04-24")
        StructField('original_language', StringType(), True),  # Language code (e.g., "en")
        StructField('budget', LongType(), True),  # Budget in USD (long for large values)
        StructField('revenue', LongType(), True),  # Revenue in USD (long for large values)
        StructField('vote_count', IntegerType(), True),  # Number of votes (integer)
        StructField('vote_average', DoubleType(), True),  # Average rating (float, e.g., 7.8)
        StructField('popularity', DoubleType(), True),  # Popularity score (float)
        StructField('runtime', IntegerType(), True),  # Runtime in minutes (integer, e.g., 181)
        StructField('overview', StringType(), True),  # Movie summary (string)
        StructField('poster_path', StringType(), True)  # Path to poster image (string)
    ]
    
    #similar to a python dictionary in pyspark it is MapType eg {"id": 299534, "name": "Harry Potter"})
    collection_field = StructField('belongs_to_collection', MapType(StringType(), StringType()), True)
    
    
    # Creates a StructField for an array of structs (e.g., list of genres).
    # ArrayType: Represents a list.
    # StructType: Defines the structure of each element in the list
    def array_struct(name, fields: List[StructField]):
        return StructField(name, ArrayType(StructType(fields)), True)
 
    
    array_fields = [
        array_struct('genres', [
            StructField('id', IntegerType(), True),  # Genre ID
            StructField('name', StringType(), True)  # Genre name (e.g., "Action")
        ]),
        array_struct('production_companies', [
            StructField('id', IntegerType(), True),  # production company id
            StructField('name', StringType(), True)  # production company name
        ]),
        array_struct('production_countries', [
            StructField('iso_3166_1', StringType(), True),  # Country code (e.g., "US")
            StructField('name', StringType(), True)  # Country name
        ]),
        array_struct('spoken_languages', [
            StructField('iso_639_1', StringType(), True),  # Language code (e.g., "en")
            StructField('name', StringType(), True)  # Language name
        ])
    ]
    '''Credits is a struct with two keys: cast and crew of which each is an array of structs like a (lists of dictionaries)
    and each struct has fields (name, character or job)
    '''
    credits_field = StructField(
        'credits',
        StructType([
            StructField('cast', ArrayType(StructType([
                StructField('name', StringType(), True),  # Actor name
                StructField('character', StringType(), True)  # Character played
            ])), True),
            StructField('crew', ArrayType(StructType([
                StructField('name', StringType(), True),  # Crew member name
                StructField('job', StringType(), True)  # Job (e.g., "Director")
            ])), True)
        ]),
        True
    )
    
    # Returns a StructType combining all fields for the DataFrame.
    return StructType(basic_fields + [collection_field] + array_fields + [credits_field])




# -------------------------------- datacleaning -----------------------------------------------



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



# -----------------------------------------------franchise analysis--------------------------------------------------



def add_is_franchise_column(df, collection_col='collection_name'):
    """
    Adds an 'is_franchise' boolean column based on whether the movie belongs to a collection.
    
    Args:
        df (DataFrame): Input PySpark DataFrame called df_with_franchise_flag
        collection_col (str): Column name containing collection info
    Returns:
        DataFrame: DataFrame with new 'is_franchise' column
    """
    return df.withColumn("is_franchise", when(col(collection_col).isNotNull(), True).otherwise(False))

def mean_revenue_by_franchise(df):
    """
    Adds an 'Mean_Revenue_musd'  column based on is_franchise column.
    
    Args:
        df (DataFrame): 'df_with_franchise_flag' the dataframe with the  PySpark DataFrame
    Returns:
        DataFrame: DataFrame with  'is_franchise' and 'Mean_Revenue_musd'  column
    """
    # Group by the 'is_franchise' column and calculate the mean of 'revenue_musd'
    mean_revenue = df.groupBy('is_franchise').agg(F.avg('revenue_musd').alias('Mean_Revenue_musd'))
    # Convert is_franchise column to 'Franchise' / 'Standalone'
    mean_revenue = mean_revenue.withColumn(
        'Is_Franchise', 
        F.when(F.col('is_franchise') == True, 'Franchise').otherwise('Standalone'))
    # Select relevant columns and return the result
    return mean_revenue.select('Is_Franchise', 'Mean_Revenue_musd')



def median_roi_by_franchise(df):
    df = df.withColumn('roi', F.col('revenue_musd') / F.col('budget_musd'))
    # Group by the 'is_franchise' column and calculate the median of 'revenue_musd'
    median_roi = df.groupBy('is_franchise').agg(F.percentile_approx('roi', 0.5, 100).alias('median_roi'))
    # Convert is_franchise column to 'Franchise' / 'Standalone'
    median_roi = median_roi.withColumn( 'Is_Franchise',F.when(F.col('is_franchise') == True, 'Franchise').otherwise('Standalone'))
    # Select relevant columns and return the result
    return median_roi.select('Is_Franchise', 'median_roi')


def mean_popularity_by_franchise(df):
     # Group by the 'is_franchise' column and calculate the mean of 'revenue_musd'
    mean_popular = df.groupBy('is_franchise').agg(F.mean('popularity').alias('Mean_popularity'))
    # Convert is_franchise column to 'Franchise' / 'Standalone'
    mean_popular = mean_popular.withColumn('Is_Franchise',F.when(F.col('is_franchise') == True, 'Franchise').otherwise('Standalone'))
    # Select relevant columns and return the result
    return mean_popular.select('Is_Franchise', 'Mean_popularity')
    

def mean_rating_by_franchise(df):
    mean_rating = df.groupBy('is_franchise').agg(F.mean('vote_average').alias('Mean_rating'))
    mean_rating= mean_rating.withColumn('Is_Franchise',F.when(F.col('is_franchise') == True, 'Franchise').otherwise('Standalone'))
    return mean_rating.select('Is_Franchise','Mean_rating')
                                             

def generate_franchise_summary(df):
    franchise_summary = df.groupBy("collection_name").agg(
        F.count("id").alias("movie_count"),
        F.sum("budget_musd").alias("total_budget"),
        F.mean("budget_musd").alias("mean_budget"),
        F.sum("revenue_musd").alias("total_revenue"),
        F.mean("revenue_musd").alias("mean_revenue"),
        F.mean("vote_average").alias("mean_rating")
    )
    return franchise_summary


def sort_mean_budget(df,collection_name,column):
    sorted_row=df.orderBy(F.col(column).desc()).first()
    print(f"With a mean budget of {sorted_row[column]:,.2f}, '{sorted_row[collection_name]}' is the most successful movie franchise.")
    return df

def sort_total_budget(df,collection_name,column):
    sorted_row=df.orderBy(F.col(column).desc()).first()
    print(f"With a  total budget of  {sorted_row[column]:,.2f}, '{sorted_row[collection_name]}' is the most sucessful movie franschise")
    return df


def sort_total_revenue(df,collection_name,column):
  sorted_row=df.orderBy(F.col(column).desc()).first()
  print(f"With a total revenue of  {sorted_row[column]:,.2f}, '{sorted_row[collection_name]}' is the most sucessful movie franschise")
  return df

def sort_mean_revenue(df,collection_name,column):
    sorted_row=df.orderBy(F.col(column).desc()).first()
    print(f"With a mean revenue of  {sorted_row[column]:,.2f}, '{sorted_row[collection_name]}' is the most sucessful movie franschise")
    return df

def sort_mean_rating(df,collection_name,column):
    sorted_row=df.orderBy(F.col(column).desc()).first()
    print(f"With a mean rating of  {sorted_row[column]:,.2f}, '{sorted_row[collection_name]}' is the most sucessful movie franschise")
    return df



def sort_most_successful_movieinfranchise(df,column, sort_column):
    sorted_row=df.orderBy(F.col(column).desc()).first()
    print(f"{sorted_row[column]} is the most successful movie franchise with  {sorted_row[sort_column]} movie franchises.")
    return df


def generate_director_df(df):
    director_summary = df.groupBy("director").agg(
        F.count("id").alias("movie_count"),
        F.sum("budget_musd").alias("total_budget"),
        F.mean("budget_musd").alias("mean_budget"),
        F.sum("revenue_musd").alias("total_revenue"),
        F.mean("revenue_musd").alias("mean_revenue"),
        F.mean("vote_average").alias("mean_rating")
    )
    return director_summary



def most_movies_directed(df, director_column, count_column):
     sorted_row=df.orderBy(F.col(count_column).desc()).first()
     return print(f"{sorted_row[director_column]} has directed {sorted_row[count_column]} movies.")


def most_successful_director_by_revenue(df,director_column, revenue_column):
     sorted_row=df.orderBy(F.col(revenue_column).desc()).first()
     return print(f"{sorted_row[director_column]} is the most successful by generating USD {sorted_row[revenue_column]} in revenue.")

def successful_director_meanrating(df, director_column, rating_column):
     sorted_row=df.orderBy(F.col(rating_column).desc()).first()
     return print(f"{sorted_row[director_column]} is the most successful by having a mean rating of {sorted_row[rating_column]}.")
 
 
 
#  -------------------------------------KPI Analysis---------------------------------------------------------------------




def highest_revenue_movie(df, title_col, revenue_col):
    # Get the row with the maximum revenue
    max_row = df.orderBy(col(revenue_col).desc()).select(title_col, revenue_col).first()
    print(f"{max_row[title_col]} generated the most revenue of USD {max_row[revenue_col]:,.2f}")
    return df

def highest_budget_movie(df, title_col, budget_col):
    # Get the row with the maximum revenue
    max_row = df.orderBy(col(budget_col).desc()).select(title_col, budget_col).first()
    print(f"{max_row[title_col]} was the most budgeted movie with  USD {max_row[budget_col]:,.2f}")
    return df


def highest_profit_movie(df, title_col, revenue_col, budget_col, profit_col='profit'):
    df = df.withColumn('profit', F.col(revenue_col) - F.col(budget_col))
    row = df.orderBy(col('profit').desc()).select(title_col, 'profit',).first()
    print(f"{row[title_col]} had the highest profit of USD {row['profit']:,.2f}")
    return df

def lowest_profit_movie(df, title_col, revenue_col, budget_col):
    df = df.withColumn("profit", F.col(revenue_col) - F.col(budget_col))
    row = df.orderBy(F.col("profit").asc()).select(title_col, 'profit',).first()
    print(f"{row[title_col]} had the lowest profit of USD {row['profit']}")
    return df

def highest_roi(df, title_col, revenue_col, budget_col, base=100):
    df = df.filter(F.col(budget_col) >= base)
    df = df.withColumn("roi", F.col(revenue_col) / F.col(budget_col))
    row = df.orderBy(F.col("roi").desc()).select(title_col, 'roi',).first()
    print(f"{row[title_col]} had the highest ROI of {row['roi']}")
    return df

def lowest_roi(df, title_col, revenue_col, budget_col, base=100):
    df = df.filter(F.col(budget_col) >= base)
    df = df.withColumn("roi", F.col(revenue_col) / F.col(budget_col))
    row = df.orderBy(F.col("roi").asc()).select(title_col, 'roi',).first()
    print(f"{row[title_col]} had the lowest ROI of {row['roi']}")
    return df

def most_voted(df, title_col, vote_col):
    row = df.orderBy(F.col(vote_col).desc()).select(title_col, vote_col,).first()
    print(f"{row[title_col]} was the most voted movie with {row[vote_col]} votes.")
    return df

def highest_rated(df, title_col, vote_col, vote_avg_col):
    df = df.filter(F.col(vote_col) >= 10)
    row = df.orderBy(F.col(vote_avg_col).desc()).select(title_col, vote_avg_col,).first()
    print(f"{row[title_col]} was the highest rated movie with a rating of {row[vote_avg_col]}")
    return df

def lowest_rated(df, title_col, vote_col, vote_avg_col):
    df = df.filter(F.col(vote_col) >= 10)
    row = df.orderBy(F.col(vote_avg_col).asc()).select(title_col, vote_avg_col).first()
    print(f"{row[title_col]} was the lowest rated movie with a rating of {row[vote_avg_col]}")
    return df

def most_popular(df, title_col, popularity_col):
    row = df.orderBy(F.col(popularity_col).desc()).select(title_col, popularity_col).first()
    print(f"{row[title_col]} was the most popular movie with a popularity score of {row[popularity_col]}")
    return df
from pyspark.sql.functions import col, expr

def advanced_search_rating(df):
    # Filter where genres contain both 'Science Fiction' and 'Action'
    genre_filter = ( (expr("array_contains(transform(genres, x -> x.name), 'Science Fiction')")) 
                    &(expr("array_contains(transform(genres, x -> x.name), 'Action')")))

    # Filter where 'Bruce Willis' is in the cast
    specific_cast__filter = expr("array_contains(transform(credits.cast, x -> x.name), 'Chris Evans')")

    # Apply filters and sort by rating
    best_rated = df \
        .filter(genre_filter & specific_cast__filter) \
        .select("title", "vote_average", "genres", "credits.cast") \
        .orderBy(col("vote_average").desc())

    # Show result
    print(f'The best-rated Science Fiction Action movies starring Chris Evans')
    best_rated.select("title", "vote_average").show(truncate=False)
    return df

def advanced_search_runtime(df):
    # Filter where 'Bruce Willis' is in the cast
    specific_cast__filter = expr("array_contains(transform(credits.cast, x -> x.name), 'Chris Evans')")

    specific_director__filter =  expr(""" exists(credits.crew, x -> x.job = 'Director' AND x.name = 'Anthony Russo')""")


    # Apply filters and sort by rating
    best_rated = df \
        .filter(specific_director__filter & specific_cast__filter) \
        .select("title", "runtime", "credits.crew", "credits.cast") \
        .orderBy(col("runtime").asc())

    # Show result
    print(f'The movies starring Uma Thurman, directed by Quentin Tarantinois with the sorted runtime')
    best_rated.select("title", "runtime").show(truncate=False)
    return df


# ---------------------------------------------------visualization------------------------------------



def revenue_vs_budget(df):
    # Convert to pandas DataFrame
    df = df.select('budget_musd', 'revenue_musd').toPandas()
    
    # Plot
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='budget_musd', y='revenue_musd')
    plt.title('Revenue vs. Budget')
    plt.xlabel('Budget (USD millions)')
    plt.ylabel('Revenue (USD millions)')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def roi_distribution_by_genre(df):
    df = df.select('genre_names', 'roi').toPandas()
    plt.figure(figsize=(30, 15))
    sns.lineplot(x = 'genre_names', y = 'roi', data = df)
    plt.xticks(rotation=90)
    plt.show()
    plt.tight_layout()
    plt.close()

def popularity_vs_rating(df):
    df = df.select('popularity', 'vote_average').toPandas()
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='popularity', y='vote_average', alpha=0.6, color='blue')
    plt.title('Popularity vs Movie Rating ')
    plt.xlabel('Popularity')
    plt.ylabel('Rating (vote_average)')
    plt.show()
    plt.close()

def yearly_box_office_performance(df):
    # Step 1: Extract year from release_date
    df_with_year = df.withColumn('release_year', year('release_date'))

    # Step 2: Group by release_year and aggregate the total revenue
    yearly_revenue = df_with_year.groupBy('release_year').agg(
        F.sum('revenue_musd').alias('Yearly_total_revenue')
    )

    # Step 3: Convert the PySpark DataFrame to Pandas DataFrame for plotting
    df_pandas = yearly_revenue.toPandas()

    # Step 4: Plot the data using seaborn and matplotlib
    plt.figure(figsize=(15, 7))
    sns.lineplot(x='release_year', y='Yearly_total_revenue', data=df_pandas, marker='o')
    plt.title('Yearly Trends in Box Office Revenue')
    plt.xlabel('Year')
    plt.ylabel('Total Revenue (in MUSD)')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    plt.close()

def franchise_vs_standalone_success(df):
    # Group by 'is_franchise' and aggregate
    summary_df = df.groupBy("is_franchise").agg(
        F.count("id").alias("movie_count"),
        F.sum("budget_musd").alias("total_budget"),
        F.mean("budget_musd").alias("mean_budget"),
        F.sum("revenue_musd").alias("total_revenue"),
        F.mean("revenue_musd").alias("mean_revenue"),
        F.mean("vote_average").alias("mean_rating")
    )

    # Convert is_franchise boolean to descriptive labels
    summary_df = summary_df.withColumn(
        "is_franchise_label",
        F.when(F.col("is_franchise") == True, "Franchise").otherwise("Standalone")
    )
    # Convert to Pandas for visualization
    pandas_summary = summary_df.toPandas()
    # Set index for plotting
    pandas_summary.set_index("is_franchise_label", inplace=True)
    # Plot bar chart
    plt.figure(figsize=(12, 7))
    pandas_summary.drop(columns=["is_franchise"]).plot(kind='bar')
    plt.title('Comparison of Franchise vs. Standalone Success')
    plt.ylabel('Average / Total Values')
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()
    plt.close()
    
    
    
    
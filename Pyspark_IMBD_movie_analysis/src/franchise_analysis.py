import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

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
    
    director_summary.coalesce(1).write.mode("overwrite").option("header", True).csv("/Users/gyauk/github/labs/IMBD_movie_analysis/Project1/data/processed/franchise_director.csv")
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
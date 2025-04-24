import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql import functions as F


def highest_revenue_movie(df, title_col, revenue_col):
    # Get the row with the maximum revenue
    max_row = df.orderBy(col(revenue_col).desc()).select(title_col, revenue_col).first()
    print(f"{max_row[title_col]} generated the most revenue of USD {max_row[revenue_col]:,.2f}")
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
    row = df.orderBy(F.col(vote_avg_col).asc()).select(title_col, vote_avg_col,).first()
    print(f"{row[title_col]} was the lowest rated movie with a rating of {row[vote_avg_col]}")
    return df

def most_popular(df, title_col, popularity_col):
    row = df.orderBy(F.col(popularity_col).desc()).select(title_col, popularity_col,).first()
    print(f"{row[title_col]} was the most popular movie with a popularity score of {row[popularity_col]}")
    return df
    
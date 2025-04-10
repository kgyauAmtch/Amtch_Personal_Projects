TMBD Data Analysis Project

Overview

This project focuses on analyzing and extracting meaningful insights from a dataset of movies, fetching the data using an API, using data cleaning, anomaly detection, and key performance indicators (KPIs). The dataset contains information about various movies, including their title, cast, crew, genres etc. 

**Table of Contents**

Fetching the Data
Data Preparation & Cleaning
Key Performance Indicators (KPIs) Analysis
Advanced Movie Filtering & Search Queries
Franchise vs. Standalone Movie Performance
Most Successful Franchises & Directors

1. Fetching the Data

The data is fetched from a CSV file, which is loaded using pandas:

import pandas as pd
import numpy as np
import ast
import seaborn as sns
import matplotlib.pyplot as plt

file_path = '/path/to/your/file.csv'
df = pd.read_csv(file_path)
df.head()
The dataset includes multiple columns, such as id, title, budget, revenue, genres, cast, and others.

2. Data Preparation & Cleaning

2.1 Drop Irrelevant Columns
Unnecessary columns such as 'adult', 'imdb_id', 'original_title', 'video', and 'homepage' are dropped:

df = df.drop(columns=['adult', 'imdb_id', 'original_title', 'video', 'homepage'])
2.2 Evaluate JSON-like Columns
The columns belongs_to_collection, genres, production_countries, production_companies, spoken_languages, and credits contain JSON-like strings. A function is applied to convert these strings into Python objects (lists or dictionaries):

def evaluate_json_column(column):
    try:
        return ast.literal_eval(column) if pd.notna(column) else {}
    except (ValueError, SyntaxError):
        return {}

json_columns = ['belongs_to_collection', 'genres', 'production_countries', 
                'production_companies', 'spoken_languages', 'credits']

for col in json_columns:
    df[col] = df[col].apply(evaluate_json_column)
2.3 Extract and Clean Key Data Points
New columns are created by extracting relevant data from JSON-like columns. For example, we extract collection_name from belongs_to_collection, and cast from credits:

def extract_collection_name(value):
    try:
        if pd.notnull(value) and isinstance(value, dict):
            return value.get('name')
    except (ValueError, SyntaxError):
        return None

df['collection_name'] = df['belongs_to_collection'].apply(extract_collection_name)
We also break data points like genres, production_countries, and production_companies into separate strings:

def break_data_points(df, init_column, new_column):
    df[new_column] = df[init_column].apply(lambda x: ' | '.join(d['name'] for d in x) if isinstance(x, list) else None)
    return df[new_column]

break_data_points(df, 'genres', 'genre_names')
break_data_points(df, 'production_countries', 'cld_production_countries')
2.4 Handle Missing & Incorrect Data
We handle missing or incorrect data by converting certain columns to numeric and datetime types, and addressing any zero values in columns like budget, revenue, and runtime.

def convert_to_numeric(df, column):
    df[column] = pd.to_numeric(df[column], errors='coerce')
    return df[column].info()

convert_to_numeric(df, 'budget')
2.5 Normalize Anomalies
We normalize string-based data columns such as genre_names, production_countries, etc., by sorting and making them unique:

def normalize_anomalies(genre_string):
    genres = list(genrestring.strip() for genrestring in genre_string.split('|'))
    sorted_genres = sorted(genres)
    return ' | '.join(sorted_genres)

df['genre_names'] = df['genre_names'].apply(normalize_anomalies)
3. Key Performance Indicators (KPIs) Analysis

The following KPIs are implemented to analyze movie performance:

3.1 Highest Revenue Movie
def highest_revenue_movie(title, revenue_column):
    max_row = reordered_df.loc[reordered_df[revenue_column].idxmax()]
    print(f'{max_row[title]} generated the most revenue of USD {max_row[revenue_column]}')
3.2 Highest Budget Movie
def highest_budget_movie(title, budget_column):
    max_row = reordered_df.loc[reordered_df[budget_column].idxmax()]
    print(f'{max_row[title]} had the highest budget of USD {max_row[budget_column]}')
3.3 Highest Profit Movie
def highest_profit_movie(title, revenue_column, budget_column, profit_column):
    reordered_df['profit'] = reordered_df[revenue_column] - reordered_df[budget_column]
    highest_profit_row = reordered_df.loc[reordered_df[profit_column].idxmax()]
    print(f'{highest_profit_row[title]} had the highest profit of USD {highest_profit_row[profit_column]}')
4. Advanced Movie Filtering & Search Queries

This section allows you to perform complex filtering queries on movies, such as finding the best-rated Science Fiction Action movies starring Bruce Willis:

specific_genres = ['Science Fiction', 'Action']
filtered_genre_df = reordered_df[reordered_df['genres'].apply(lambda genres: any(genre['name'] in specific_genres for genre in genres))]
filter_actor_df = filtered_genre_df[filtered_genre_df['cast'].apply(lambda cast: 'Bruce Willis' in cast)]
sorted_movies = filter_actor_df.sort_values(by='vote_average', ascending=False)
5. Franchise vs. Standalone Movie Performance

This section compares the performance of franchise movies versus standalone movies by analyzing mean revenue, budget, popularity, and rating:

df['is_franchise'] = df['collection_name'].notna()

mean_revenue_comparison = df.groupby('is_franchise')['revenue_musd'].mean().reset_index()
mean_revenue_comparison.columns = ['Is_Franchise', 'Mean_Revenue_musd']
mean_revenue_comparison['Is_Franchise'] = mean_revenue_comparison['Is_Franchise'].map({True: 'Franchise', False: 'Standalone'})
6. Most Successful Franchises & Directors

This section identifies the most successful franchises and directors based on various metrics, including total revenue, mean revenue, and mean rating.

franchise_counts = df['collection_name'].value_counts()
print('Most successful franchise is', franchise_counts.idxmax(), 'with', franchise_counts.max(), 'movies in a franchise')
Conclusion

This project provides comprehensive analysis and insights into movie data, allowing for comparisons between different categories of movies (e.g., franchise vs. standalone) and identifying key factors contributing to their success. The results can inform decision-making, marketing strategies, and financial planning in the movie industry.
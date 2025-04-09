import pandas as pd

def add_is_franchise_column(df):
    df['is_franchise'] = df['collection_name'].notna()
    return df['is_franchise'].head()

def mean_revenue_by_franchise(df):
    df = add_is_franchise_column(df)
    mean_revenue = df.groupby('is_franchise')['revenue_musd'].mean().reset_index()
    mean_revenue.columns = ['Is_Franchise', 'Mean_Revenue_musd']
    mean_revenue['Is_Franchise'] = mean_revenue['Is_Franchise'].map({True: 'Franchise', False: 'Standalone'})
    return mean_revenue

def median_roi_by_franchise(df):
    df['roi'] = df['revenue_musd'] / df['budget_musd']
    df = add_is_franchise_column(df)
    median_roi = df.groupby('is_franchise')['roi'].median().reset_index()
    median_roi.columns = ['Is_Franchise', 'Median_ROI']
    median_roi['Is_Franchise'] = median_roi['Is_Franchise'].map({True: 'Franchise', False: 'Standalone'})
    return median_roi

def mean_popularity_by_franchise(df):
    df = add_is_franchise_column(df)
    mean_popularity = df.groupby('is_franchise')['popularity'].mean().reset_index()
    mean_popularity.columns = ['Is_Franchise', 'Mean_Popularity']
    mean_popularity['Is_Franchise'] = mean_popularity['Is_Franchise'].map({True: 'Franchise', False: 'Standalone'})
    return mean_popularity

def mean_rating_by_franchise(df):
    df = add_is_franchise_column(df)
    mean_rating = df.groupby('is_franchise')['vote_average'].mean().reset_index()
    mean_rating.columns = ['Is_Franchise', 'Mean_Vote_Average']
    mean_rating['Is_Franchise'] = mean_rating['Is_Franchise'].map({True: 'Franchise', False: 'Standalone'})
    return mean_rating

def generate_franchise_summary(df):
    franchise_summary = df.groupby('collection_name').agg(
        movie_count=('id', 'count'),
        total_budget=('budget_musd', 'sum'),
        mean_budget=('budget_musd', 'mean'),
        total_revenue=('revenue_musd', 'sum'),
        mean_revenue=('revenue_musd', 'mean'),
        mean_rating=('vote_average', 'mean')
    ).reset_index()
    return franchise_summary

def sort_most_successful_franchise(df, collection_column, sort_column):
    sorted_df = df.sort_values(sort_column)
    return print(f"{sorted_df[collection_column].iloc[-1]} is the most successful movie franchise based on {sort_column}.")

def generate_director_summary(df):
    director_summary = df.groupby('director').agg(
        num_movies_directed=('id', 'count'),
        total_revenue=('revenue_musd', 'sum'),
        mean_rating=('vote_average', 'mean')
    ).reset_index()
    return director_summary

def most_movies_directed(df, director_column, count_column):
    sorted_df = df.sort_values(count_column)
    return print(f"{sorted_df[director_column].iloc[-1]} has directed {sorted_df[count_column].iloc[-1]} movies.")

def most_successful_director_by_revenue(df, director_column, revenue_column):
    sorted_df = df.sort_values(revenue_column)
    return print(f"{sorted_df[director_column].iloc[-1]} is the most successful by generating USD {sorted_df[revenue_column].iloc[-1]} in revenue.")

def most_successful_director_by_rating(df, director_column, rating_column):
    sorted_df = df.sort_values(rating_column)
    return print(f"{sorted_df[director_column].iloc[-1]} is the most successful by having a mean rating of {sorted_df[rating_column].iloc[-1]}.")
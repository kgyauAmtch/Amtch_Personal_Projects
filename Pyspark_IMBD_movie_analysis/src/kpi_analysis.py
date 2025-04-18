import pandas as pd

def highest_revenue_movie(df, title, revenue_column):
    max_row = df.loc[df[revenue_column].idxmax()]
    print(f"{max_row[title]} generated the most revenue of USD {max_row[revenue_column]}")

def highest_budget_movie(df, title, budget_column):
    max_row = df.loc[df[budget_column].idxmax()]
    print(f"{max_row[title]} had the highest budget of USD {max_row[budget_column]}")

def highest_profit_movie(df, title, revenue_column, budget_column, profit_column='profit'):
    df[profit_column] = df[revenue_column] - df[budget_column]
    highest_profit_row = df.loc[df[profit_column].idxmax()]
    print(f"{highest_profit_row[title]} had the highest profit of USD {highest_profit_row[profit_column]}")

def lowest_profit_movie(df, title, revenue_column, budget_column, profit_column='profit'):
    df[profit_column] = df[revenue_column] - df[budget_column]
    lowest_profit_row = df.loc[df[profit_column].idxmin()]
    print(f"{lowest_profit_row[title]} had the lowest profit of USD {lowest_profit_row[profit_column]}")

def highest_roi(df, title, revenue_column, budget_column, roi_column='roi', base=100):
    df_roi_range = df[df[budget_column] >= base]
    df_roi_range[roi_column] = df_roi_range[revenue_column] / df_roi_range[budget_column]
    highest_roi_row = df_roi_range.loc[df_roi_range[roi_column].idxmax()]
    print(f"{highest_roi_row[title]} had the highest ROI of {highest_roi_row[roi_column]}")

def lowest_roi(df, title, revenue_column, budget_column, roi_column='roi', base=100):
    df_roi_range = df[df[budget_column] >= base]
    df_roi_range[roi_column] = df_roi_range[revenue_column] / df_roi_range[budget_column]
    lowest_roi_row = df_roi_range.loc[df_roi_range[roi_column].idxmin()]
    print(f"{lowest_roi_row[title]} had the lowest ROI of {lowest_roi_row[roi_column]}")

def most_voted(df, title, vote_column):
    most_voted_row = df.loc[df[vote_column].idxmax()]
    print(f"{most_voted_row[title]} was the most voted movie with {most_voted_row[vote_column]} votes.")

def highest_rated(df, title, vote_column, vote_average):
    df_rated_range = df[df[vote_column] >= 10]
    highest_rated_row = df_rated_range.loc[df_rated_range[vote_average].idxmax()]
    print(f"{highest_rated_row[title]} was the highest rated movie with a rating of {highest_rated_row[vote_average]}")

def lowest_rated(df, title, vote_column, vote_average):
    df_rated_range = df[df[vote_column] >= 10]
    lowest_rated_row = df_rated_range.loc[df_rated_range[vote_average].idxmin()]
    print(f"{lowest_rated_row[title]} was the lowest rated movie with a rating of {lowest_rated_row[vote_average]}")

def most_popular(df, title, popularity_column):
    most_popular_row = df.loc[df[popularity_column].idxmax()]
    print(f"{most_popular_row[title]} was the most popular movie with a popularity score of {most_popular_row[popularity_column]}")

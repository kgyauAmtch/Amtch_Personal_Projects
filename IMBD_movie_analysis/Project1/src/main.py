import pandas as pd
from kpi_analysis import (
    highest_revenue_movie,
    highest_budget_movie,
    highest_profit_movie,
    lowest_profit_movie,
    highest_roi,
    lowest_roi,
    most_voted,
    highest_rated,
    lowest_rated,
    most_popular
)

from franchise_analysis import (
    most_movies_directed,
    sort_mean_budget,
    sort_total_budget,
    sort_total_revenue,
    sort_mean_revenue,
    sort_mean_rating,
    sort_most_successful_movieinfranchise,
    most_successful_director_by_revenue,
    successful_director_meanrating
)

from visualisation import (
    revenue_vs_budget,
    roi_distribution_by_genre,
    popularity_vs_rating,
    yearly_box_office_performance,
    franchise_vs_standalone_success
)

def main():
    print("Loading data...")
    df = pd.read_csv('/Users/gyauk/github/Project1_movie_analysis/Project1/data/processed/movies.csv')
    reordered_df =pd.read_csv('/Users/gyauk/github/Project1_movie_analysis/Project1/data/processed/reordered_movies.csv')
    franchise_summary_df=pd.read_csv('/Users/gyauk/github/Project1_movie_analysis/Project1/data/processed/franchise.csv')
    franchise_director=pd.read_csv('/Users/gyauk/github/Project1_movie_analysis/Project1/data/processed/franchise_director.csv')
   
    # --- KPIs ---
    print("\n--- Key Performance Indicators ---")
    highest_revenue_movie(reordered_df,'title', 'revenue_musd')
    highest_budget_movie(reordered_df,'title', 'budget_musd')
    highest_profit_movie(reordered_df,'title', 'revenue_musd', 'budget_musd', 'profit')
    lowest_profit_movie(reordered_df,'title', 'revenue_musd', 'budget_musd', 'profit')
    highest_roi(reordered_df,'title', 'revenue_musd', 'budget_musd', 'roi')
    lowest_roi(reordered_df,'title', 'revenue_musd', 'budget_musd', 'roi')
    most_voted(reordered_df,'title', 'vote_count')
    highest_rated(reordered_df,'title', 'vote_count', 'vote_average')
    lowest_rated(reordered_df,'title', 'vote_count', 'vote_average')
    most_popular(reordered_df,'title', 'popularity')

    # --- Franchise vs Standalone ---
    print("\n--- Franchise Analysis ---")
    most_movies_directed(franchise_director,'num_movies_directed','director')
    sort_most_successful_movieinfranchise(franchise_summary_df,'collection_name','movie_count')
    sort_mean_budget(franchise_summary_df,'collection_name','mean_revenue')
    sort_total_budget(franchise_summary_df,'collection_name','total_budget')
    sort_total_revenue(franchise_summary_df,'collection_name','total_revenue')
    sort_mean_revenue(franchise_summary_df,'collection_name','mean_revenue')
    sort_mean_rating(franchise_summary_df,'collection_name','mean_rating')
    sort_most_successful_movieinfranchise(franchise_summary_df,'collection_name','movie_count')
    most_successful_director_by_revenue(franchise_director,'director','total_revenue')
    successful_director_meanrating(franchise_director,'director','mean_rating')

    # --- Visualizations ---
    print("\n--- Generating Visualizations ---")
    revenue_vs_budget(reordered_df)
    roi_distribution_by_genre(reordered_df)
    popularity_vs_rating(reordered_df)
    yearly_box_office_performance(reordered_df)
    # franchise_vs_standalone_success(df)

    print("\nAll tasks completed!")

if __name__ == '__main__':
    main()

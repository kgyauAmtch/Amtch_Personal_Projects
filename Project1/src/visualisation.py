import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


def revenue_vs_budget(df):
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='budget_musd', y='revenue_musd', hue='is_franchise')
    plt.title('Revenue vs. Budget')
    plt.xlabel('Budget (USD millions)')
    plt.ylabel('Revenue (USD millions)')
    plt.close()

def roi_distribution_by_genre(df):
    plt.figure(figsize=(30, 15))
    sns.lineplot(x = 'genre_names', y = 'roi', data = reordered_df)
    plt.xticks(rotation=90)
    plt.show()
    plt.tight_layout()
    plt.close()

def popularity_vs_rating(df):
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='popularity', y='vote_average', alpha=0.6, color='blue')
    plt.title('Popularity vs Movie Rating ')
    plt.xlabel('Popularity')
    plt.ylabel('Rating (vote_average)')
    plt.show()
    plt.close()

def yearly_box_office_performance(df):
    reordered_df['release_year'] = reordered_df['release_date'].dt.year
    plt.figure(figsize=(15, 7)) # To resize the plot
    yearly_df = reordered_df.groupby('release_year').agg({
        'revenue_musd': 'sum'
    }).reset_index()
    sns.pointplot(x='release_year', y='revenue_musd', data=yearly_df)
    plt.legend(bbox_to_anchor=(1, 1))
    plt.xticks(rotation=90)
    plt.show()
    plt.close()

def franchise_vs_standalone_success(df):
    plt.figure(figsize=(10, 6))
    franchise_group = df.groupby('is_franchise').agg({
        'revenue_musd': 'mean',
        'roi': 'median',
        'popularity': 'mean',
        'vote_average': 'mean'
    }).reset_index()

    franchise_group['is_franchise'] = franchise_group['is_franchise'].map({True: 'Franchise', False: 'Standalone'})
    franchise_group.set_index('is_franchise', inplace=True)
    franchise_group.plot(kind='bar', figsize=(12, 7))
    plt.title('Comparison of Franchise vs. Standalone Success')
    plt.ylabel('Average / Median Values')
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig('images/franchise_vs_standalone_success.png')
    plt.close()
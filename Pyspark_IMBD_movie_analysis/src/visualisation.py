import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql.functions import year
from pyspark.sql  import functions as F

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


# from pyspark.sql.functions import year, sum as Fsum

# def yearly_box_office_performance(df):
#     # Step 1: Extract year
#     df_with_year = df.withColumn('release_year', year('release_date'))

#     # Step 2: Group by year and calculate total revenue
#     yearly_revenue = df_with_year.groupBy('release_year').agg(
#         Fsum('revenue_musd').alias('total_revenue_musd')
#     )

#     # Optional: Order the results by year for better readability
#     yearly_revenue = yearly_revenue.orderBy('release_year')

#     return yearly_revenue


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
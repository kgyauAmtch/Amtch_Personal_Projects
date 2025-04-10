
# Movie Data Analysis Project

## Overview

This project focuses on comprehensive analysis of the TMBD movie trends, compare franchise vs. standalone movies, and evaluate key performance indicators (KPIs).
---

## Project Structure

- **data/**: Contains raw and cleaned datasets.
- **notebooks/**: Contains jupyter notebook for analysis.
- **src/**: Python scripts for modular code.

---

## Data Source

The dataset includes information such as:
- Movie titles
- Budget
- Revenue
- Release dates
- Genres
- Production companies
- Cast and crew
- Runtime
- Ratings etc

---

## Key Stages of Analysis

### 1. Data Cleaning and Preparation

- Removal of duplicates and null values
- Conversion of currency and date formats
- Normalization of nested dictionaries like genres, cast, directors.

### 2. Feature Engineering

- Extracting release year and decade
- Calculating ROI and profitability
- Creating binary indicators for franchises and genres
- Identifying top actors, directors, and producers

### 3. KPI Evaluation

- Budget vs. Revenue analysis
- Profitability trends over time
- ROI by genre and company
- Runtime distributions by genre
- Seasonal performance (month-wise releases)

### 4. Franchise vs. Standalone Comparison

- Average revenue and ROI for franchise vs. non-franchise films
- Budget distribution differences
- Hit rate and volume trends across years

### 5. Advanced Filtering and Insights

- High-revenue outliers and blockbuster clusters
- Company-wise performance benchmarking
- Genre popularity shifts over time
- Impact of director and actor combinations

---

## Tools Used

- Python (Pandas, NumPy, Matplotlib, Seaborn)
- Jupyter Notebooks
- Parquet for optimized data storage

---

## Insights and Conclusions

- **Franchises** tend to have higher average budgets and revenue, but not always better ROI.
- **Genres** like Action and Adventure consistently outperform others in revenue.
- **Top production companies** dominate high-grossing films, but some independents show strong ROI.
- **Seasonal trends** indicate higher success rates in summer and holiday seasons.
- **Star power and proven directors** positively correlate with box office success.

---

## Future Work

- Incorporate streaming data for modern release analysis
- Sentiment analysis of reviews and social media
- Time series forecasting for box office trends


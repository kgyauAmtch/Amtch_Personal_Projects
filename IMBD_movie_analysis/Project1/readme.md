
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

- Extracting release year 
- Calculating ROI 
- Creating indicators for franchises and genres
- Identifying top actors, directors, and producers

### 3. KPI Evaluation

- Budget vs. Revenue analysis
- Profitability trends over time
- ROI by genre and company
- Runtime distributions by genre
- Seasonal performance (month-wise releases)

### 4. Franchise vs. Standalone Comparison

- Mean Revenue
- Median ROI
- Mean Budget Raised
- Mean Popularity
- Mean Rating

### 5. Advanced Filtering and Insights

- Revenue vs. Budget Trends
- ROI Distribution by Genre
- Popularity vs. Rating
- Yearly Trends in Box Office Performance
- Comparison of Franchise vs. Standalone Success


---

## Tools Used

- Python (Pandas, NumPy, Matplotlib, Seaborn)
- Jupyter Notebooks
- Parquet for optimized data storage



---


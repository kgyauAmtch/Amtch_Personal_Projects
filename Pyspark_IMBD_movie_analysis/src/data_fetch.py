import requests
import os
from dotenv import load_dotenv
from datetime import datetime
from pyspark.sql import SparkSession

# Load environment variables
load_dotenv()
API_KEY = os.getenv('API_KEY')

# API details
api_url = "https://api.themoviedb.org/3/movie/"
movie_ids = [0, 299534, 19995, 140607, 299536, 597, 135397,
             420818, 24428, 168259, 99861, 284054, 12445,
             181808, 330457, 351286, 109445, 321612, 260513]

# Initialize SparkSession
spark = SparkSession.builder.appName("MovieAPIAnalysis").getOrCreate()

# Function to fetch movie data
def fetch_movie(movie_id):
    url = f"{api_url}{movie_id}?api_key={API_KEY}&append_to_response=credits,directors"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Movie ID {movie_id} not found.")
        print(url)
        return None

# Fetch all movies into a list
movies_data = []
for movie_id in movie_ids:
    data = fetch_movie(movie_id)
    if data:
        movies_data.append(data)

# If no data, stop the script
if not movies_data:
    print("No movie data fetched. Exiting.")
    exit()

# Convert list of dicts to an RDD
rdd = spark.sparkContext.parallelize(movies_data)

# Convert RDD to DataFrame (infer schema from JSON structure)
movies_df = spark.read.json(rdd)

# Show sample data
movies_df.show(5, truncate=False)

# Save to CSV
timestamp = datetime.now().strftime('%d-%m-%Y_%H-%M-%S')
output_path = f"/Users/gyauk/github/labs/Pyspark_IMBD_movie_analysis/data/raw/movies_{timestamp}.csv"

movies_df.write.option("header", True).csv(output_path)

print(f"Saved to {output_path}")

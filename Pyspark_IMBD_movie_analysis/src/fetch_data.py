import requests
import json
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from schema import schema_build  # schema function is in schema.py

# 1. Setup
load_dotenv()
API_KEY = os.getenv('API_KEY')
api_url = "https://api.themoviedb.org/3/movie/"
movie_ids = [0, 299534, 19995, 140607, 299536, 597, 135397,
             420818, 24428, 168259, 99861, 284054, 12445,
             181808, 330457, 351286, 109445, 321612, 260513]

# 2. Fetch function
def fetch_movie(movie_id):
    url = f"{api_url}{movie_id}?api_key={API_KEY}&append_to_response=credits"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch Movie ID {movie_id}: {e}")
        return None

# 3. Fetch data
movies_data = [fetch_movie(mid) for mid in movie_ids if fetch_movie(mid)]

# 4. Save to NDJSON (JSON Lines)
json_path = "/Users/gyauk/github/labs/Pyspark_IMBD_movie_analysis/data/raw/movies.json"
with open(json_path, "w") as f:
    for movie in movies_data:
        json.dump(movie, f)
        f.write("\n")

# 5. Load into PySpark
spark = SparkSession.builder \
    .appName("TMDB Movie Loader") \
    .getOrCreate()

df = spark.read.schema(schema_build()).json(json_path)

df.printSchema()
df.show(truncate=False)

# # Define output file path
# output_path = "/Users/gyauk/github/labs/Pyspark_IMBD_movie_analysis/data/raw/movies.json"

# # Save DataFrame as JSON file
# df.write.mode("overwrite").json(output_path)

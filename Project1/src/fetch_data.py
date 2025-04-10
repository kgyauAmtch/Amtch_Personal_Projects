import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime 

load_dotenv()
API_KEY = os.getenv('API_KEY')


# API_KEY = "c55dbe93df0a88e0f3757ca2282ebef2"
api_url = "https://api.themoviedb.org/3/movie/"

movie_ids = [0, 299534, 19995, 140607, 299536, 597, 135397,
            420818, 24428, 168259, 99861, 284054, 12445,
            181808, 330457, 351286, 109445, 321612, 260513]

def fetch_movie(movie_id):
    url = f"{api_url}{movie_id}?api_key={API_KEY}&append_to_response=credits,directors"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    
    else:
        print(f"Movie ID {movie_id} not found.")
        print(url)
        return None
   
# print(url)
    
# Fetch all movies
movies_data = []
for movie_id in movie_ids:
    data = fetch_movie(movie_id)
    if data:
        movies_data.append(data)

# Convert to DataFrame
df = pd.DataFrame(movies_data)

# Show top rows
print(df.head())



# Optionally save to file
# timestamp= datetime.now().strftime('%d-%m-%Y_%H-%M-%S')
# df.to_csv(f"/Users/gyauk/github/Project1_movie_analysis/Project1/data/raw/movies_{timestamp}.csv", index=False)
df.to_csv(f"/Users/gyauk/github/Project1_movie_analysis/Project1/data/raw/movies.csv", index=False)



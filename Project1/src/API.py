import requests


API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJjNTVkYmU5M2RmMGE4OGUwZjM3NTdjYTIyODJlYmVmMiIsIm5iZiI6MTc0Mzg1NDExNS45NzcsInN1YiI6IjY3ZjExYTIzYjNlMDM1Mjg2Y2Q5OTA2YSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.2E7dQx-CxRQv-I2tEoYujvZHQWFRWoYX81UVYy6qxvo" 
api_url = "https://api.themoviedb.org/3/authentication/token/new"

movie_ids = [0, 299534, 19995, 140607, 299536, 597, 135397,
            420818, 24428, 168259, 99861, 284054, 12445,
            181808, 330457, 351286, 109445, 321612, 260513]


def fetch_movie(movie_id):
    url = f"{api_url}{movie_id}?api_key={API_KEY}"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Movie ID {movie_id} not found.")
        return None
    
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
df.to_csv("movies.csv", index=False)
from pyspark.sql.functions import *
from pyspark.sql.types import *


def schema_build():
    """
    Define the schema for the movie DataFrame based on TMDB API response structure.
    
    Returns:
        StructType: Spark schema for movie data
    """
    basic_fields = [
        StructField('id', IntegerType(), False),  # Movie ID (non-nullable integer)
        StructField('title', StringType(), True),  # Movie title (string, nullable)
        StructField('tagline', StringType(), True),  # Movie tagline (string, nullable)
        StructField('release_date', StringType(), True),  # Release date (string, e.g., "2019-04-24")
        StructField('original_language', StringType(), True),  # Language code (e.g., "en")
        StructField('budget', LongType(), True),  # Budget in USD (long for large values)
        StructField('revenue', LongType(), True),  # Revenue in USD (long for large values)
        StructField('vote_count', IntegerType(), True),  # Number of votes (integer)
        StructField('vote_average', DoubleType(), True),  # Average rating (float, e.g., 7.8)
        StructField('popularity', DoubleType(), True),  # Popularity score (float)
        StructField('runtime', IntegerType(), True),  # Runtime in minutes (integer, e.g., 181)
        StructField('overview', StringType(), True),  # Movie summary (string)
        StructField('poster_path', StringType(), True)  # Path to poster image (string)
    ]
    
    collection_field = StructField('belongs_to_collection', MapType(StringType(), StringType()), True)
    # MapType for collection metadata (e.g., {"id": 123, "name": "Avengers Collection"}).
    
    def array_struct(name: str, fields: List[StructField]) -> StructField:
        return StructField(name, ArrayType(StructType(fields)), True)
    # Creates a StructField for an array of structs (e.g., list of genres).
    # ArrayType: Represents a list.
    # StructType: Defines the structure of each element in the list.
    
    array_fields = [
        array_struct('genres', [
            StructField('id', IntegerType(), True),  # Genre ID
            StructField('name', StringType(), True)  # Genre name (e.g., "Action")
        ]),
        array_struct('production_companies', [
            StructField('id', IntegerType(), True),  # Company ID
            StructField('name', StringType(), True)  # Company name
        ]),
        array_struct('production_countries', [
            StructField('iso_3166_1', StringType(), True),  # Country code (e.g., "US")
            StructField('name', StringType(), True)  # Country name
        ]),
        array_struct('spoken_languages', [
            StructField('iso_639_1', StringType(), True),  # Language code (e.g., "en")
            StructField('name', StringType(), True)  # Language name
        ])
    ]
    
    credits_field = StructField(
        'credits',
        StructType([
            StructField('cast', ArrayType(StructType([
                StructField('name', StringType(), True),  # Actor name
                StructField('character', StringType(), True)  # Character played
            ])), True),
            StructField('crew', ArrayType(StructType([
                StructField('name', StringType(), True),  # Crew member name
                StructField('job', StringType(), True)  # Job (e.g., "Director")
            ])), True)
        ]),
        True
    )
    
    return StructType(basic_fields + [collection_field] + array_fields + [credits_field])
    # Returns a StructType combining all fields for the DataFrame.
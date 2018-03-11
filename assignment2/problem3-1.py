import json
import os
import pandas as pd
import elasticsearch
from elasticsearch import helpers

es = elasticsearch.Elasticsearch() # Defines how to connect to a Elasticsearch node

data_dir = "./movielens"

df_dbpedia = pd.read_csv(os.path.join(data_dir, "dbpedia.csv"))
df_dbpedia["dbpedia_content"] = df_dbpedia["dbpedia_content"].apply(json.loads) # Parse string to JSON
df_movie = pd.read_csv(os.path.join(data_dir, "movies.csv"))
df_movie["genres"] = df_movie["genres"].apply(lambda x: x.replace("|",","))
df_rating = pd.read_csv(os.path.join(data_dir, "ratings.csv"))
df_user = pd.read_csv(os.path.join(data_dir, "users.csv"))

df_dbpedia_merged = df_dbpedia[["movie_id","dbpedia_content"]].merge(df_movie, on="movie_id")

if es.indices.exists("movies"):
    es.indices.delete("movies")

def fast_load(bulk_size = 10000):
    tasks = []
    for index, movie in df_dbpedia_merged.iterrows():
        try:
            my_movie = {}
            my_movie["abstract"] = movie["dbpedia_content"]["abstract"]
            my_movie["title"] = movie["title"]
            my_movie["genres"] = movie["genres"]
            if "subject" in movie["dbpedia_content"]:
                my_movie["subject"] = movie["dbpedia_content"]["subject"]
            if "starring" in movie["dbpedia_content"]:
                my_movie["starring"] = movie["dbpedia_content"]["starring"]

            to_add = {
                "_index": "movies",
                "_type": "movie",
                "_source": my_movie,
                "_id": movie["movie_id"]
            }
            
            tasks.append(to_add)
            if len(tasks) % bulk_size == 0:
                helpers.bulk(es, tasks)
                tasks = []
        except Exception as ex:
            print(str(ex))
    helpers.bulk(es, tasks)

fast_load()
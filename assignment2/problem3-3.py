import json
import os
import pandas as pd
import elasticsearch
import math
from elasticsearch import helpers

es = elasticsearch.Elasticsearch() # Defines how to connect to a Elasticsearch node

data_dir = "./data/movielens"

df_dbpedia = pd.read_csv(os.path.join(data_dir, "dbpedia.csv"))
df_dbpedia["dbpedia_content"] = df_dbpedia["dbpedia_content"].apply(json.loads) # Parse string to JSON
df_movie = pd.read_csv(os.path.join(data_dir, "movies.csv"))
df_movie["genres"] = df_movie["genres"].apply(lambda x: x.replace("|",","))
df_rating = pd.read_csv(os.path.join(data_dir, "ratings.csv"))
df_user = pd.read_csv(os.path.join(data_dir, "users.csv"))

df_dbpedia_merged = df_dbpedia[["movie_id","dbpedia_content"]].merge(df_movie, on="movie_id")

# if es.indices.exists("movies"):
#     es.indices.delete("movies")

# es.indices.create(index="movies",
#                                 body={
#                                     "mappings": {
#                                         "movie": {
#                                             "properties": {
#                                                 "abstract": {
#                                                     "type": "text",
#                                                     "analyzer": "english",
#                                                     "term_vector": "yes"
#                                                 },
#                                                 "genres": {
#                                                     "type": "text",
#                                                     "fields": {
#                                                         "keyword": {
#                                                             "type": "keyword",
#                                                             "ignore_above": 256
#                                                         }
#                                                     }
#                                                 },
#                                                 "starring": {
#                                                     "type": "string",
#                                                     "index": "not_analyzed"
#                                                 },
#                                                 "subject": {
#                                                     "type": "string",
#                                                     "index": "not_analyzed"
#                                                 },
#                                                 "title": {
#                                                     "type": "text",
#                                                     "fields": {
#                                                         "keyword": {
#                                                             "type": "keyword",
#                                                             "ignore_above": 256
#                                                         }
#                                                     }
#                                                 }
#                                             }
#                                         }
#                                     },
#                                     "settings": {
#                                         "index": {
#                                             "number_of_shards": "1"
#                                         }
#                                     }
#                                }
#                  )


# def fast_load(bulk_size = 10000):
#     tasks = []
#     for index, movie in df_dbpedia_merged.iterrows():
#         try:
#             my_movie = {}
#             my_movie["abstract"] = movie["dbpedia_content"]["abstract"]
#             my_movie["title"] = movie["title"]
#             my_movie["genres"] = movie["genres"]
#             if "subject" in movie["dbpedia_content"]:
#                 my_movie["subject"] = movie["dbpedia_content"]["subject"]
#             if "starring" in movie["dbpedia_content"]:
#                 my_movie["starring"] = movie["dbpedia_content"]["starring"]

#             to_add = {
#                 "_index": "movies",
#                 "_type": "movie",
#                 "_source": my_movie,
#                 "_id": movie["movie_id"]
#             }

#             tasks.append(to_add)
#             if len(tasks) % bulk_size == 0:
#                 helpers.bulk(es, tasks)
#                 tasks = []
#         except Exception as ex:
#             print(str(ex))
#     helpers.bulk(es, tasks)

# fast_load()

def tf_idf_weight(freq_of_term_in_docs, num_of_docs_contain_term, documents):
    return (1 + math.log(freq_of_term_in_docs)) * math.log10(documents/num_of_docs_contain_term) 


def tf_idf(index, doc_type, doc_id, field):
    term_vectors = es.termvectors(index="movies", doc_type="movie", id=doc_id, params={"fields": field})["term_vectors"][field]["terms"]

    number_of_documents =  es.count(index="movies") # get count of all documents fix this to wait for all documents to be loaded

    all_ids = list(map(lambda x: x["_id"], es.search(index="movies", doc_type="movie", body={"size": 4000, "query": {"match_all": {}}, "stored_fields": ["_id"]})["hits"]["hits"]))

    term_frequencies = es.mtermvectors(index="movies", doc_type="movie", params={"ids": all_ids, "fields": field})

    for term in term_vectors:
        number_of_documents_containing_term = es.count(index="movies", body={
            "query": {
                "term": {
                    field: term
                }
            }
        })
        

    return []

print(tf_idf("movies", "movie", 2, "abstract"))

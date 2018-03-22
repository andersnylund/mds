# Modern Database Systems CS-E4610 - Assignment 2
## **Anders Nylund 659888**

## Part A: Pen and Paper

### Problem 1.1

#### Question 1

**Solution 1:**
To determine the importance of a document (web-page), we need to have all other documents on hand. 

As the search engine will produce search result, we could collect statistic on what results gets used the most. This means that when the engine produces results when searching for something, we could collect "clicks" on how many times the user selects a particular result. This way we could rank each document by how many times it has been clicked, no matter what the query was.

When we know how many times a document has been selected as the "best result" by the user we increase a counter. So then each document will be ranked by the number of "clicks" and a number between 0 and 1 will be given to it, where the most clicked document has the value 1, and the least clicked value 0. 

The problem with this is that old documents often dominate the "leaderboard" of importance. Therefore there is a need to introduce some kind of different time intervals, as today, this week, this month and all time.

**Solution 2:**
By monetizing the importance of the document, the publishers of the documents can pay to the search engine provider. By giving for example the possibility to have 4 different classes of importance where free is class 0, and 1-3 are gradually more expensive.

Here the importance boost of paying can for example be made obsolete 4 weeks, and after that the importance goes down to 0 again. 

#### Question 2
As web-pages are markup they often follow the same conventions everywhere. In fact, web-pages are often on purpose implemented with "good practices" to enable search-engine indexing as good as possible. Therefore common tags like `<h1>`, `<body>` and `<p>` are used in a way that makes sense. 

However, these tags are quite standard and new rules are constantly emerging.

There is a lot of other things that can be considered also. ShoutMeLoud (2017) lists 11 tips of how to increase the visibility of your webpage showing up. These are not taking de-facto standard things like the HTML tags into consideration.

For example google has not officially listed their indexing rules, but after time the basic rules has been formed and 

### Question 3


## Part B: Programming

### Problem 3.1

**Solution:**
```
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
```

**Mapping created by ElasticSearch:**
```
"mappings": {
    "movie": {
        "properties": {
            "abstract": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "genres": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "starring": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "subject": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "title": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            }
        }
    }
}
```

### Problem 3.2
**Solution:**

I copied the default mapping created by Elasticearch in problem 3.1 and modified the properties of that to match the requirements. By modifying the "subject" and "starring" properties' indices to "not_analyzed" we get the desired behavior

Python code to create the desired mapping:
```
es.indices.create(index="movies",
                                body={
                                    "mappings": {
                                        "movie": {
                                            "properties": {
                                                "abstract": {
                                                    "type": "text",
                                                    "fields": {
                                                        "keyword": {
                                                            "type": "keyword",
                                                            "ignore_above": 256
                                                        }
                                                    }
                                                },
                                                "genres": {
                                                    "type": "text",
                                                    "fields": {
                                                        "keyword": {
                                                            "type": "keyword",
                                                            "ignore_above": 256
                                                        }
                                                    }
                                                },
                                                "starring": {
                                                    "type": "string",
                                                    "index": "not_analyzed"
                                                },
                                                "subject": {
                                                    "type": "string",
                                                    "index": "not_analyzed"
                                                },
                                                "title": {
                                                    "type": "text",
                                                    "fields": {
                                                        "keyword": {
                                                            "type": "keyword",
                                                            "ignore_above": 256
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                   }
                               }
                 )

```

### Problem 3.3 1)

**Full mapping:**

```
"mappings": {
    "movie": {
        "properties": {
            "abstract": {
                "type": "text",
                "analyzer": "english",
                "term_vector": "yes"
            },
            "genres": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "starring": {
                "type": "string",
                "index": "not_analyzed"
            },
            "subject": {
                "type": "string",
                "index": "not_analyzed"
            },
            "title": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            }
        }
    }
},
"settings": {
    "index": {
        "number_of_shards": "1"
    }
}
```

**Python tf_idf function**
```
def tf_idf_weight(freq_of_term_in_docs, num_of_documents, num_of_docs_contain_term):
    return (1 + math.log(freq_of_term_in_docs)) * math.log10(num_of_documents/num_of_docs_contain_term) 


def tf_idf(index, doc_type, doc_id, field):
    term_idf_list = []
    
    params = {
        "fields": field,
        "term_statistics": True,
        "field_statistics": True
    }
    
    document_term_vectors = es.termvectors(index="movies", doc_type="movie", id=doc_id, params=params)

    number_of_all_documents = document_term_vectors["term_vectors"][field]["field_statistics"]["doc_count"]

    for term, value in document_term_vectors["term_vectors"][field]["terms"].items():
        weight = tf_idf_weight(value["ttf"], number_of_all_documents, value["doc_freq"])
        term_idf_list.append((term, weight))
        
    term_idf_list.sort(key=lambda tup: tup[1], reverse=True)
    return term_idf_list
```

The result for  tf_idf(“movies”, “movie”, 2, “abstract”):
```
[
    ('game', 9.477052402943105), 
    ('hunt', 9.379976906997754), 
    ('comput', 9.307755643571701), 
    ('van', 9.265620875802805), 
    ('johnson', 9.196046604654528), 
    ('price', 9.176363555382164), 
    ('robin', 9.166564719135216), 
    ('judi', 9.161818288976658), 
    ('pierc', 9.158876154488267), 
    ('light', 9.126801831003398), 
    ('magic', 9.10003801368827), 
    ('yet', 9.087459659000638), 
    ('graphic', 9.083114163180133), 
    ('1969', 9.08020112758106), 
    ('board', 9.08020112758106), 
    ('track', 9.071322620957192), 
    ('industri', 9.07122095778509), 
    ('columbia', 9.056856873764595), 
    ('caus', 9.032303072682897), 
    ('15', 9.032212333556568)
]
```

### Problem 3.3 2)

Solution to Problem 3.3 1) used the _english_-analyzer that understands the English grammar. This way it removes stopwords as "the", "a" etc. Additionally it will stem the words and try to transform them to their root form. 

Therefore a weight schema produced with the _english_-analyzer might produce a "better" schema, as for example, compared by using the _whitespace_-analyzer. The _english_-analyzer is able to take into account different forms of the same word and therefore produce a more accurate weight schema.

### Problem 3.4 1)

```
def more_like_this(document_id):
    body = { 
        "query": {
            "more_like_this": {
                "fields" : ["starring", "subjects", "abstract"],
                "like" : [
                    {
                        "_id": document_id
                    }
                ],
                "min_term_freq" : 1,
                "max_query_terms" : 10
            }
        }
    }

    return es.search(index="movies", doc_type="movie", body=body)
```

### Problem 3.4 2)

The limitation of _max_query_terms_, as the name explains, limits the terms used in the query, and therefore by lowering the amount makes the query concentrate on smaller amount of terms. Therefore for example, with a low _max_query_terms_ value, the query might give a high value for a movie that has the same most frequent term on the abstract field.

Setting the the min_term_freq forces the query to only include terms that have high frequency, and therefore the result might be more accurate when it considers only terms that have a high frequency 


## References

ShoutMeLoud, (2017). _11 Solid Tips to Increase Google Crawl Rate Of Your Website_. [online]. [viewed on: 19 march 2018] 
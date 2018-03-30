import findspark
findspark.init()

import pyspark
spark = pyspark.SparkContext(master="local[*]", appName="movie")

lines = spark.textFile("./data/movielens/ratings.csv")

# remove header, create a tuple of each row, groupByKey and mapValues reduces by key and sums the count of each movie)
interm = lines.map(lambda line: line.split(",")) \
    .filter(lambda line: line[0] != "user_id") \
    .map(lambda line: (int(line[1]), 1)) \
    .groupByKey() \
    .mapValues(len) \
    .collect()
interm.sort(key=lambda tup: tup[0]) # sort the list by movie_id

max_ratings = max(interm, key=lambda item: item[1])[1] # get movie with most ratings
result = list(filter(lambda movie: movie[1]/max_ratings >= 0.25, interm)) # filter list with 25th percentile

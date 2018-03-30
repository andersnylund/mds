import findspark
findspark.init()

import pyspark
spark = pyspark.SparkContext(master="local[*]", appName="movie")

lines = spark.textFile("./data/movielens/ratings.csv")

result = lines.map(lambda line: line.split(",")) \ 
    .filter(lambda line: line[0] != "user_id") \
    .map(lambda line: (int(line[1]), 1)) \
    .groupByKey() \
    .mapValues(len) \
    .collect()

result.sort(key=lambda tup: tup[0])

import pyspark
from pyspark import SparkContext
from pyspark.sql.types import FloatType


spark = pyspark.sql.SparkSession.builder \
    .master("local") \
    .appName("movies") \
    .getOrCreate()

df = spark.read.csv(path="./comparison.csv", header=True)

df = df.withColumn("similarity", df["similarity"].cast(FloatType()))\
    .orderBy("similarity", ascending=[0]) \
    .collect()

sc = SparkContext.getOrCreate()
rdd = sc.parallelize(df)


def seq_op(acc, row):
    similarity = row["similarity"]

    if row["first"] in acc:
        acc[row["first"]].append((row["second"], similarity))
    else:
        acc[row["first"]] = [(row["second"], similarity)]

    if row["second"] in acc:
        acc[row["second"]].append((row["first"], similarity))
    else:
        acc[row["second"]] = [(row["first"], similarity)]

    return acc


similarities = rdd.aggregate({}, seq_op, lambda x, y: {**x, **y})


def map_averages(acc, row):
    acc[row["user"]] = row["average"]
    return acc


averages = spark.read.csv(path="./averages.csv", header=True).rdd.aggregate({}, map_averages, lambda x, y: {**x, **y})


movies = spark.read.csv(path='./data/movielens/movies.csv', header=True).collect()


def mapping(user, nearest):

    predicted_ratings = []
    user_average = averages[user]

    for movie in movies:
        for other in nearest:
            


sc.parallelize(similarities).map(lambda user: mapping(user, similarities[user][:10])).collect()

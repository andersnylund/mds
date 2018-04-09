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
        acc[row["first"]] = [(row["first"], similarity)]

    if row["second"] in acc:
        acc[row["second"]].append((row["first"], similarity))
    else:
        acc[row["second"]] = [(row["second"], similarity)]

    return acc


result = rdd.aggregate({}, seq_op, lambda x, y: {**x, **y})


for user in result:
    sorted(result[user])
    result[user] = result[user][:10]


print(result)

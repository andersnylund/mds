import findspark
findspark.init()
import pyspark
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType

spark = pyspark.sql.SparkSession.builder \
    .master("local") \
    .appName("movies") \
    .getOrCreate()

df = spark.read.csv(path="./data/movielens/ratings.csv", header=True)
df = df.withColumn("rating", df["rating"].cast(IntegerType()))

averages = df \
    .groupBy("user_id") \
    .avg("rating") \
    .select("*")

subtracted = averages \
    .join(df, df["user_id"] == averages["user_id"]) \
    .select("*")

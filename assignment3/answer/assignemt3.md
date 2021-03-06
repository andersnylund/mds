# Modern Database Systems CS-E4610 - Assignment 3
## **Anders Nylund 659888**

## Part A: Pen and Paper

### Problem 1

#### Question 1

## Part B: Programming

### Problem 4

#### Task 1

```
import findspark
findspark.init()
import pyspark
import pyspark.sql.functions as func

spark = pyspark.sql.SparkSession.builder \
    .master("local") \
    .appName("movies") \
    .getOrCreate()

df = spark.read.csv(path="./data/movielens/ratings.csv", header=True)

max_ratings = df.groupBy("movie_id") \
    .agg(func.count(func.lit(1)).alias("ratings")) \
    .agg({"ratings": "max"}) \
    .collect()[0]["max(ratings)"]

twenty5th_percentile = df.groupBy("movie_id") \
    .agg(func.count(func.lit(1)).alias("ratings")) \
    .filter("ratings/"+str(max_ratings)+" > 0.25") \
    .orderBy("movie_id") \
    .collect()
```




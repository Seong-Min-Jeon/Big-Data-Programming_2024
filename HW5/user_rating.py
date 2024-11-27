from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HW5").getOrCreate()

df1 = spark.read.csv("ml-latest-small/ratings.csv", header=True)
df1.createOrReplaceTempView("ratings")

df2 = spark.read.csv("ml-latest-small/movies.csv", header=True)
df2.createOrReplaceTempView("movies")

result = spark.sql("""
WITH ten_rating_users AS (
    SELECT userId
    FROM ratings
    GROUP BY userId
    HAVING COUNT(DISTINCT movieId) >= 10
),
filtering_rating AS (
    SELECT r.*
    FROM ratings r
    JOIN ten_rating_users t ON r.userId = t.userId
),
join_movie AS (
    SELECT
      m.title,
      ROUND(AVG(f.rating), 2) AS average_rating,
      COUNT(f.userId) AS rating_count
    FROM filtering_rating f
    JOIN movies m ON f.movieId = m.movieId
    GROUP BY f.movieId, m.title
    HAVING AVG(f.rating) < 2.0 AND COUNT(f.userId) >= 30
)
SELECT
  title,
  average_rating,
  rating_count
FROM join_movie
ORDER BY rating_count DESC
""")

result_df = result.select("title", "average_rating", "rating_count")

import pandas as pd
result_pd = result_df.toPandas()

for index, row in result_pd.iterrows():
    print(f"{row['title']}\t{row['average_rating']}\t{row['rating_count']}")

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HW5_2").getOrCreate()

ratings_rdd = spark.read.csv("ml-latest-small/ratings.csv", 
                                header=True).rdd
movies_rdd = spark.read.csv("ml-latest-small/movies.csv", 
                                header=True).rdd

user_movie_count = ratings_rdd \
                    .map(lambda row: (row['userId'], row['movieId'])) \
                    .distinct() \
                    .mapValues(lambda _: 1) \
                    .reduceByKey(lambda a, b: a + b)

ten_rating_users = user_movie_count.filter(lambda x: x[1] >= 10) \
                    .map(lambda x: x[0]).collect()

filtering_rating = ratings_rdd \
                    .filter(lambda row: row['userId'] in ten_rating_users)

movie_ratings = filtering_rating \
                    .map(lambda row: (row['movieId'], 
                                     (float(row['rating']), 1))) \
                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

movie_avg_count = movie_ratings.mapValues(lambda x: (x[0] / x[1], x[1]))
filtering_movie = movie_avg_count \
                    .filter(lambda x: x[1][0] < 2.0 and x[1][1] >= 30)

movie_titles = movies_rdd.map(lambda row: (row['movieId'], row['title']))
movie_join = filtering_movie.join(movie_titles)

result = movie_join.map(lambda x: (x[1][1], round(x[1][0][0],2), x[1][0][1]))

final_result = result.distinct().sortBy(lambda x: x[2], ascending=False)

for row in final_result.collect():
    print(f"{row[0]}\t{row[1]}\t{row[2]}")

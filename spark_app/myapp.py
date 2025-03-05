from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, desc, avg, dense_rank, col, count, trim, round
from pyspark.sql.window import Window

def main():
    with SparkSession.builder.appName("MyApp").getOrCreate() as spark:
        df = spark.read.parquet("s3://${ACCOUNT_ID}-formatted-data")

        # preprocessing
        df = df.filter(~(col("movie").rlike("\\(\\d{4}–") | col("movie").like("%Episode%")))
        df = df.withColumn("movie_year", regexp_extract(df.movie, r"\((\d{4})", 1)).withColumn("movie_title", trim(regexp_extract("movie", r"^(.*?)\((\d{4})", 1)))
        df = df.filter(col("movie_year") != "")
        # calculating avd_rating and num_of_reviews
        movies_with_avg_rating = df.groupBy("movie_title", "movie_year").agg(round(avg("rating"),2).alias("avg_rating"), count("movie_title").alias("num_of_reviews")).sort(
            desc("avg_rating"))
        # filter out movies with less reviews
        movies_with_avg_rating = movies_with_avg_rating.filter(movies_with_avg_rating.num_of_reviews > 5)
        # partition by movie_year, sort by avg_rating and num_of_reviews
        movie_ranking_by_year_window = Window.partitionBy("movie_year").orderBy(col("avg_rating").desc(), col("num_of_reviews").desc())
        # dense_rank 1 for each year
        top_movies_for_each_year = movies_with_avg_rating.withColumn("ranking", dense_rank().over(movie_ranking_by_year_window)).filter(
            col("ranking") <= 3).orderBy(col("movie_year").desc(), col("ranking").asc(), col("num_of_reviews").desc())
            
        # saving json file for top movies
        top_movies_for_each_year.coalesce(1).write.json("s3://${ACCOUNT_ID}-result/results.json", mode='overwrite')


if __name__ == "__main__":
    main()
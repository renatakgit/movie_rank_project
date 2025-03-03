WITH ranked_movies AS (
  SELECT 
    movie,
    -- extract movie title by removing the year part in parentheses
    TRIM(REGEXP_EXTRACT(movie, '^(.*?)\((\d{4})', 1)) AS movie_title,
    -- extract the release year from the movie column
    REGEXP_EXTRACT(movie, '\((\d{4})', 1) AS movie_year,
    -- calculate the average rating rounded to 2 decimal places
    ROUND(AVG(rating), 2) AS avg_rating,
    -- count the number of reviews for each movie
    COUNT(movie) AS nb_of_reviews,
    -- rank movies within the same year based on average rating and number of reviews
    DENSE_RANK() OVER (
      PARTITION BY REGEXP_EXTRACT(movie, '\((\d{4})', 1) 
      ORDER BY ROUND(AVG(rating), 2) DESC, COUNT(movie) DESC
    ) AS ranking
  FROM "student"."reviews"
  -- exclude tv series (denoted by a year range) and episodes
  WHERE NOT (REGEXP_LIKE(movie, '\(\d{4}–') OR movie LIKE '%Episode%')
  -- group by movie title and year to aggregate ratings and review count
  GROUP BY
    movie,
    TRIM(REGEXP_EXTRACT(movie, '^(.*?)\((\d{4})', 1)),
    REGEXP_EXTRACT(movie, '\((\d{4})', 1) 
  -- filter out movies with 5 or fewer reviews and those missing a release year
  HAVING COUNT(movie) > 5 AND REGEXP_EXTRACT(movie, '\((\d{4})', 1) IS NOT NULL
)

-- select the top 3 ranked movies for each year
SELECT *
FROM ranked_movies
WHERE ranking <= 3
ORDER BY movie_year DESC, ranking;
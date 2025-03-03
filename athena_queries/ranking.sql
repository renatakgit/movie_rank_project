WITH ranked_movies AS (
SELECT 
    movie,
    TRIM(REGEXP_EXTRACT(movie, '^(.*?)\((\d{4})', 1)) AS movie_title,
    REGEXP_EXTRACT(movie, '\((\d{4})', 1) AS movie_year,
    ROUND(AVG(rating), 2) AS avg_rating,
    COUNT(movie) AS nb_of_reviews,
    DENSE_RANK() OVER (PARTITION BY REGEXP_EXTRACT(movie, '\((\d{4})', 1) ORDER BY ROUND(AVG(rating), 2) DESC, COUNT(movie) DESC) AS ranking
FROM "student"."reviews"
WHERE NOT (REGEXP_LIKE(movie, '\(\d{4}–') OR movie LIKE '%Episode%')
GROUP BY
    movie,
    TRIM(REGEXP_EXTRACT(movie, '^(.*?)\((\d{4})', 1)),
    REGEXP_EXTRACT(movie, '\((\d{4})', 1) 
HAVING COUNT(movie) >5 AND REGEXP_EXTRACT(movie, '\((\d{4})', 1) IS NOT NULL
)

SELECT *
FROM ranked_movies
WHERE ranking <= 3
ORDER BY movie_year DESC, ranking;
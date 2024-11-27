WITH genre_split AS (
    SELECT 
        m.movieId,
        m.title,
        TRIM(REGEXP_SUBSTR(m.genres, '[^|]+', 1, LEVEL)) AS genre
    FROM 
        movies m
    CONNECT BY 
        PRIOR m.movieId = m.movieId 
        AND PRIOR SYS_GUID() IS NOT NULL
        AND LEVEL <= LENGTH(m.genres) - LENGTH(REPLACE(m.genres, '|', '')) + 1
),
table_join AS (
    SELECT movieId, title, genre, rating FROM genre_split NATURAL JOIN ratings
),
genre_rating AS (
    SELECT         
        genre,
        COUNT(rating) AS count_rating,
        ROUND(AVG(rating),2) AS avg_rating
    FROM 
        table_join
    GROUP BY 
        genre
    HAVING 
        COUNT(rating) >= 30
)
SELECT * FROM genre_rating ORDER BY count_rating DESC;
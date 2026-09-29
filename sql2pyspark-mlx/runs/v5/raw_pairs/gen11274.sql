WITH base AS (
    SELECT *, FLOOR(rating) AS rating_whole
    FROM suppliers
    WHERE country != 'SG'
),
rating_bucket_summary AS (
    SELECT CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, ROUND(AVG(rating), 2) AS avg_rating, AVG(rating_whole) AS avg_rating_whole
    FROM base
    GROUP BY CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END
)
SELECT rating_bucket, avg_rating, avg_rating_whole
FROM rating_bucket_summary
ORDER BY avg_rating DESC, rating_bucket DESC

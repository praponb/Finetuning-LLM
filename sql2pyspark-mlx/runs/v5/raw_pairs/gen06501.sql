WITH base AS (
    SELECT *
    FROM suppliers
    WHERE country IN ('TH', 'US')
),
suppliers_stats AS (
    SELECT CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END rating_bucket, COUNT(DISTINCT rating) unique_citys, AVG(rating) avg_rating
    FROM base
    GROUP BY CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END
)
SELECT rating_bucket, unique_citys, avg_rating
FROM suppliers_stats

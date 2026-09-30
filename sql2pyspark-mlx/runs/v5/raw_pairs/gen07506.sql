WITH metrics AS (
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        LEAD(rating, 1, 0) OVER (PARTITION BY country ORDER BY rating, id) AS next_rating
    FROM suppliers
    WHERE rating > 4.0
)
SELECT
    city,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    COUNT(*) FILTER (WHERE country = 'US') AS us_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM metrics
GROUP BY city
ORDER BY us_rows NULLS FIRST;

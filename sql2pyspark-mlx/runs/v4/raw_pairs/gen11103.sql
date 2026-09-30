SELECT
    country,
    APPROX_PERCENTILE(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) AS rating_per_bangkok
FROM suppliers
GROUP BY country

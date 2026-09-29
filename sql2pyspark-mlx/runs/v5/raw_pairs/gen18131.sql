SELECT *
FROM (
    SELECT CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket, country, ROUND(AVG(rating), 2) AS avg_rating
    FROM suppliers
    GROUP BY CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END, country
) AS agg
WHERE avg_rating >= 26
ORDER BY country DESC;

SELECT
    CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.6 THEN 'mid' ELSE 'low' END AS rating_bucket,
    ROUND(STDDEV(rating), 2) AS stddev_rating,
    SUM(CASE WHEN city = 'Tokyo' THEN rating ELSE 0 END) AS tokyo_rating,
    SUM(rating) AS total_rating
FROM suppliers
WHERE supplier_name IS NOT NULL
GROUP BY rating_bucket

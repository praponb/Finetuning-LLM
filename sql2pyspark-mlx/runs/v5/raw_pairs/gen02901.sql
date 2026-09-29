SELECT
    city,
    SIZE(COLLECT_SET(city)) AS num_cities,
    PERCENTILE_APPROX(rating, 0.95) AS rating_p95
FROM suppliers
GROUP BY city
HAVING num_cities > 1

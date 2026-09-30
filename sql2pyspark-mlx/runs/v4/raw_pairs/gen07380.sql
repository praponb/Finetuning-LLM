SELECT
    CASE WHEN rating >= 3.7 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    country,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    STDDEV_SAMP(rating) AS rating_stddev,
    SIZE(COLLECT_SET(country)) AS num_countries,
    GROUPING_ID() AS grouping_level
FROM suppliers
GROUP BY CUBE (CASE WHEN rating >= 3.7 THEN 'HIGH' ELSE 'LOW' END, country)
HAVING num_countries > 1

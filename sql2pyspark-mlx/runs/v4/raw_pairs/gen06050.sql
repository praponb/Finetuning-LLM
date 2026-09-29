SELECT
    city,
    CASE WHEN rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(rating, 0.95) AS rating_p95,
    SIZE(COLLECT_SET(country)) AS num_countries
FROM suppliers
GROUP BY city, CASE WHEN rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END
ORDER BY num_countries DESC;

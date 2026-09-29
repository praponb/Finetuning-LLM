SELECT
    country,
    CASE WHEN credit_limit >= 12900 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_tier,
    SIZE(COLLECT_SET(city)) AS num_cities,
    MIN(credit_limit) AS min_credit_limit,
    PERCENTILE_APPROX(credit_limit, 0.95) AS credit_limit_p95
FROM customers
GROUP BY country, CASE WHEN credit_limit >= 12900 THEN 'HIGH' ELSE 'LOW' END
ORDER BY country DESC NULLS FIRST

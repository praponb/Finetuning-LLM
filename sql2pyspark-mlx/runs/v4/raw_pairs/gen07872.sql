SELECT
    country,
    COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count,
    PERCENTILE_APPROX(credit_limit, 0.95) AS credit_limit_p95,
    GROUPING(country) AS grouping_id
FROM customers
GROUP BY ROLLUP (country);

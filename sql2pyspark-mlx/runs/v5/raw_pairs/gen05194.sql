SELECT
    CASE WHEN credit_limit >= 15600 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    country,
    APPROX_PERCENTILE(age, 0.95) AS age_p95,
    COUNT_IF(country = 'DE') AS de_events,
    SUM(CASE WHEN segment = 'SMB' THEN credit_limit ELSE 0 END) AS smb_credit_limit,
    MIN(age) AS min_age,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    COUNT(DISTINCT country) AS distinct_countries,
    GROUPING(CASE WHEN credit_limit >= 15600 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM customers
GROUP BY ROLLUP (CASE WHEN credit_limit >= 15600 THEN 'PREMIUM' ELSE 'STANDARD' END, country);

SELECT
    CASE WHEN credit_limit >= 17100 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    CASE WHEN age >= 65 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
    APPROX_PERCENTILE(credit_limit, 0.9) AS credit_limit_p90,
    COUNT(DISTINCT country) AS distinct_countries,
    BIT_AND(age) AS age_bit_and,
    GROUPING_ID() AS grouping_id
FROM customers
GROUP BY GROUPING SETS ((CASE WHEN credit_limit >= 17100 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN age >= 65 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN credit_limit >= 17100 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY age_tier;

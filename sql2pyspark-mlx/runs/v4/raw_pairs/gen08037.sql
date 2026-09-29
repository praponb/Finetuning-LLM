SELECT
    CASE WHEN credit_limit >= 16900 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
    MIN(age) AS min_age,
    COUNT(*) FILTER (WHERE segment = 'Consumer') AS consumer_rows,
    COUNT(DISTINCT country) AS distinct_countries
FROM customers
GROUP BY credit_limit_tier
HAVING min_age >= 0
ORDER BY credit_limit_tier NULLS LAST;

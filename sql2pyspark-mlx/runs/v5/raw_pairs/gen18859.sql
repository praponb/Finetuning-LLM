SELECT
    CASE WHEN credit_limit >= 15100 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS credit_limit_per_new_york,
    SUM(CASE WHEN segment = 'Corporate' THEN credit_limit ELSE 0 END) AS corporate_credit_limit,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT_IF(city = 'Chiang Mai') AS chiang_mai_events,
    ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS age_per_singapore,
    AVG(credit_limit) FILTER (WHERE segment = 'SMB') AS smb_avg_credit_limit,
    GROUPING_ID() AS grouping_id
FROM customers
GROUP BY ROLLUP (CASE WHEN credit_limit >= 15100 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', signup_date))
HAVING AVG(credit_limit) FILTER (WHERE segment = 'SMB') < 1000000
ORDER BY age_per_singapore;

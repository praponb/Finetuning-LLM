SELECT
    CASE WHEN credit_limit >= 12900 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    COUNT(DISTINCT segment) AS distinct_segments,
    SUM(credit_limit) AS total_credit_limit,
    ROUND(AVG(age), 2) AS avg_age,
    COUNT(*) FILTER (WHERE segment = 'Consumer') AS consumer_rows
FROM customers
GROUP BY 1, 2

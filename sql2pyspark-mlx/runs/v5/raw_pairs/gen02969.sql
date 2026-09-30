SELECT
    CASE WHEN segment IN ('Corporate', 'Consumer') THEN 'tier_1' ELSE 'other' END AS segment_group,
    city,
    COUNT(*) AS num_customers,
    SUM(CASE WHEN country = 'UK' THEN credit_limit ELSE 0 END) AS uk_credit_limit,
    MAX(signup_date) AS latest_signup_date
FROM customers
WHERE age IS NOT NULL
GROUP BY segment_group, city
ORDER BY 5

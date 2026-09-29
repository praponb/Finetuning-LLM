SELECT
    is_active,
    SUM(credit_limit) AS total_credit_limit,
    COUNT(*) AS cnt,
    SUM(SUM(credit_limit)) OVER (ORDER BY is_active) AS cumulative_credit_limit
FROM customers
GROUP BY is_active

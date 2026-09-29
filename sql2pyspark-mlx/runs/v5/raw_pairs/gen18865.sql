SELECT
    is_active,
    SUM(credit_limit) AS total_credit_limit,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(credit_limit) DESC) AS rnk
FROM customers
WHERE credit_limit IS NOT NULL
GROUP BY is_active
ORDER BY rnk DESC

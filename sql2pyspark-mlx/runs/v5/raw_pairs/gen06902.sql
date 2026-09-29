SELECT
    id,
    name,
    email,
    is_active,
    age,
    MAX(credit_limit) OVER (PARTITION BY country, segment ORDER BY signup_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_credit_limit,
    AVG(credit_limit) OVER (PARTITION BY country, segment ORDER BY signup_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_credit_limit,
    SUM(credit_limit) OVER (PARTITION BY country, segment ORDER BY signup_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_credit_limit
FROM customers
WHERE UPPER(TRIM(name)) = 'DAVID LEE'

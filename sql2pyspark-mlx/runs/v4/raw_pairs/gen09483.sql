SELECT
    id,
    country,
    segment,
    signup_date,
    credit_limit,
    SUM(credit_limit) OVER (PARTITION BY country, segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit
FROM customers

SELECT
    id,
    city,
    signup_date,
    age,
    SUM(age) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age
FROM customers
WHERE credit_limit < 12800

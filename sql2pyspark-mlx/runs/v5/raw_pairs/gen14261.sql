SELECT
    id,
    segment,
    signup_date,
    age,
    SUM(age) OVER (PARTITION BY segment ORDER BY signup_date, id) AS running_age
FROM customers
WHERE credit_limit IS NULL
ORDER BY signup_date, running_age

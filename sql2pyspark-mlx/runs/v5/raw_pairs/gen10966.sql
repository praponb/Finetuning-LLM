SELECT
    id,
    country,
    signup_date,
    age,
    LAG(age, 1, 0) OVER (PARTITION BY country ORDER BY signup_date, id) AS prev_age
FROM customers

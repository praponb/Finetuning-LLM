SELECT
    id,
    city,
    signup_date,
    age,
    LAG(age, 1) OVER (PARTITION BY city ORDER BY signup_date, id) AS prev_age
FROM customers
ORDER BY prev_age DESC;

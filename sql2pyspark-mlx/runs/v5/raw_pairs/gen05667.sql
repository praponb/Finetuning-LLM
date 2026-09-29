SELECT
    id,
    city,
    country,
    signup_date,
    age,
    LEAD(age, 1) OVER (PARTITION BY city, country ORDER BY signup_date, id) AS next_age
FROM customers;

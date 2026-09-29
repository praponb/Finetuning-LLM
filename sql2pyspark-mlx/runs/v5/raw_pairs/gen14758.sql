SELECT
    id,
    name,
    email,
    country,
    city,
    credit_limit,
    age,
    SUM(age) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age,
    DENSE_RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_dense_rank,
    LAG(age, 1, 0) OVER (PARTITION BY segment, country ORDER BY signup_date, id) AS prev_age,
    FIRST_VALUE(age, TRUE) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age
FROM customers
WHERE age <= 32;

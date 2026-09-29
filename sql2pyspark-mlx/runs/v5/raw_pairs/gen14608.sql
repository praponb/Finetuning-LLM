SELECT
    id,
    segment,
    signup_date,
    credit_limit,
    age,
    SUM(credit_limit) OVER (PARTITION BY segment ORDER BY signup_date, id) AS running_credit_limit,
    SUM(age) OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age
FROM customers;

SELECT
    id,
    segment,
    signup_date,
    DENSE_RANK() OVER (PARTITION BY segment ORDER BY signup_date DESC) AS signup_date_dense_rank
FROM customers;

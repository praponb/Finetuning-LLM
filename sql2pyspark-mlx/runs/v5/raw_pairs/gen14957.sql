SELECT
    id,
    segment,
    age,
    country,
    signup_date,
    AVG(age) OVER (PARTITION BY segment) AS segment_avg_age,
    DENSE_RANK() OVER (PARTITION BY country ORDER BY signup_date) AS signup_date_dense_rank
FROM customers;

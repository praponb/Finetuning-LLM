SELECT
    id,
    segment,
    credit_limit,
    AVG(credit_limit) OVER (PARTITION BY segment) AS segment_avg_credit_limit
FROM customers
WHERE city = 'Bangkok'

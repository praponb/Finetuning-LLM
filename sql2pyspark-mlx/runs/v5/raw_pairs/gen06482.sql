SELECT
    id,
    city,
    credit_limit,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS credit_limit_dense_rank
FROM customers
WHERE LENGTH(email) > 10
ORDER BY city DESC

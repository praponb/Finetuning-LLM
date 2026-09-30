SELECT
    id,
    segment,
    credit_limit,
    DENSE_RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC) AS dense_rnk
FROM customers

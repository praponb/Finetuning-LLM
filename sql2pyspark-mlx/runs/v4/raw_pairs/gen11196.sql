SELECT
    id,
    customer_id,
    balance,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY balance DESC) AS balance_dense_rank
FROM accounts;

SELECT
    id,
    opened_date,
    customer_id,
    balance,
    FIRST_VALUE(customer_id) OVER (ORDER BY opened_date, id) AS first_customer_id,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY balance DESC) AS balance_dense_rank
FROM accounts;

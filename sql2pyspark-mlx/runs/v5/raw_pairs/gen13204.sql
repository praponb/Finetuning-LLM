SELECT
    id,
    customer_id,
    balance,
    opened_date,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance, id) AS balance_row,
    DENSE_RANK() OVER (ORDER BY opened_date) AS opened_date_dense_rank
FROM accounts
WHERE balance >= 75000

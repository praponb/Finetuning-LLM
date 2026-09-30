SELECT id, merchant, amount, DENSE_RANK() OVER (PARTITION BY merchant ORDER BY amount) AS dense_rnk FROM transactions WHERE txn_time < '2024-01-21'

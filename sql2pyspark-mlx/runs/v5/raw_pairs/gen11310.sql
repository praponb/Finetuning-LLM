SELECT
    *,
    FIRST_VALUE(status) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status,
    COUNT(balance) OVER (PARTITION BY customer_id) AS customer_id_count_balance
FROM accounts

SELECT
    id,
    customer_id,
    account_type,
    balance,
    status,
    FIRST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance,
    SUM(balance) OVER (PARTITION BY account_type ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance
FROM accounts;

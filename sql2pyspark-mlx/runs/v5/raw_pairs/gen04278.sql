SELECT
    id,
    customer_id,
    balance,
    opened_date,
    status,
    FIRST_VALUE(balance) IGNORE NULLS OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_balance,
    SUM(balance) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance
FROM accounts;

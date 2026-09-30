SELECT
    customer_id,
    opened_date,
    balance,
    ABS(balance) AS abs_balance
FROM accounts
WHERE status IN ('FROZEN', 'OPEN') AND customer_id IN (9, 34, 39);

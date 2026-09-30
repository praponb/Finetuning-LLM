SELECT
    id,
    customer_id,
    opened_date,
    balance,
    LEAD(balance, 2) OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS next_balance
FROM accounts
WHERE opened_date < '2023-04-01'

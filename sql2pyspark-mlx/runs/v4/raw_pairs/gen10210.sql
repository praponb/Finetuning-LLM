SELECT
    id,
    status,
    opened_date,
    balance,
    account_type,
    LEAD(balance, 1, 0) OVER (PARTITION BY status ORDER BY opened_date, id) AS next_balance,
    RANK() OVER (PARTITION BY account_type ORDER BY balance DESC) AS balance_rank
FROM accounts
WHERE customer_id IS NULL;

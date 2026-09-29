SELECT
    id,
    account_type,
    opened_date,
    balance,
    customer_id,
    LEAD(balance, 1) OVER (PARTITION BY account_type ORDER BY opened_date, id) AS next_balance,
    PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY balance) AS balance_pct_rank
FROM accounts
ORDER BY id DESC;

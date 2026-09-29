SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    FIRST_VALUE(balance, TRUE) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance,
    CUME_DIST() OVER (ORDER BY balance NULLS LAST) AS balance_cume_dist,
    LAST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance
FROM accounts

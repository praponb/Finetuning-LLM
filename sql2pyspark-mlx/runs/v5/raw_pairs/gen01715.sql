SELECT
    id,
    opened_date,
    status,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS row_seq,
    LAST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY account_type, customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance,
    FIRST_VALUE(status) IGNORE NULLS OVER (PARTITION BY customer_id, status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status
FROM accounts
WHERE balance BETWEEN 20000 AND 68000;

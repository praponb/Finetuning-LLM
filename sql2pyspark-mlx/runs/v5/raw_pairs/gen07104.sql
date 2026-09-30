SELECT
    *,
    SUM(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance,
    PERCENT_RANK() OVER (PARTITION BY account_type ORDER BY balance NULLS LAST) balance_percent_rank,
    ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY opened_date, id) seq_no
FROM accounts
WHERE status <> 'FROZEN';

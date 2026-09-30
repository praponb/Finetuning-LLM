SELECT
    id,
    merchant,
    txn_time,
    PERCENT_RANK() OVER (PARTITION BY merchant ORDER BY txn_time) AS txn_time_pct_rank
FROM transactions
WHERE account_id = 6

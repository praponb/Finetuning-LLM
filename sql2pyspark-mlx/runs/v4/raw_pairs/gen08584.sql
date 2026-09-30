SELECT
    id,
    txn_type,
    txn_time,
    PERCENT_RANK() OVER (PARTITION BY txn_type ORDER BY txn_time) AS txn_time_pct_rank
FROM transactions;

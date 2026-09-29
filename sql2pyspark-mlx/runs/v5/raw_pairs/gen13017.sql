SELECT
    account_id,
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    COUNT(*) AS row_count,
    MAX(txn_time) AS last_seen,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    COUNT(DISTINCT account_id) AS distinct_account_ids
FROM transactions
GROUP BY account_id, DATE_TRUNC('QUARTER', txn_time)
HAVING COUNT(DISTINCT account_id) > 1

SELECT
    account_id,
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    CASE WHEN amount >= 4200 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    COUNT(*) AS cnt,
    MAX(amount) FILTER (WHERE txn_type = 'CREDIT') AS credit_max_amount,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    AVG(amount) FILTER (WHERE txn_type = 'CREDIT') AS credit_avg_amount,
    COUNT_IF(txn_type = 'FEE') AS fee_events
FROM transactions
GROUP BY account_id, DATE_TRUNC('WEEK', txn_time), CASE WHEN amount >= 4200 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY duration_min DESC;

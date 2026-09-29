SELECT
    merchant,
    account_id,
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    COUNT(*) AS event_count,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN merchant = '7-Eleven' THEN 1 END), 0), 2) AS amount_per_7_eleven
FROM transactions
GROUP BY merchant, account_id, DATE_TRUNC('YEAR', txn_time)
HAVING COUNT(*) >= 1
ORDER BY event_count DESC;

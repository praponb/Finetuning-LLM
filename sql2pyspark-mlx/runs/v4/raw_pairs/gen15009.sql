SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    PERCENTILE_APPROX(amount, 0.95) AS amount_p95,
    COUNT_IF(txn_type = 'FEE') AS fee_events,
    ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
GROUP BY DATE_TRUNC('YEAR', txn_time), DATE_TRUNC('WEEK', txn_time)
HAVING ROUND(AVG(amount), 2) < 0

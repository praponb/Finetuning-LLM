SELECT
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    CASE WHEN amount >= 4400 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    AVG(amount) AS avg_amount,
    PERCENTILE_APPROX(amount, 0.5) AS amount_p50,
    COUNT(*) FILTER (WHERE txn_type = 'DEBIT') AS debit_rows,
    SUM(amount) FILTER (WHERE txn_type = 'CREDIT') AS credit_total_amount
FROM transactions
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', txn_time), DATE_TRUNC('YEAR', txn_time), CASE WHEN amount >= 4400 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('QUARTER', txn_time), DATE_TRUNC('YEAR', txn_time)), ())
ORDER BY amount_p50 DESC NULLS FIRST

WITH prev_events AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
gaps AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    CASE WHEN burst_id >= 7 THEN 'LARGE' ELSE 'SMALL' END AS burst_id_tier,
    CASE WHEN new_group_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END AS new_group_flag_tier,
    COUNT(CASE WHEN merchant = 'Starbucks' THEN 1 END) AS starbucks_count,
    COUNT_IF(merchant = 'Amazon') AS amazon_events,
    COUNT(*) FILTER (WHERE txn_type = 'DEBIT') AS debit_rows,
    COUNT(*) AS event_count,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', txn_time) ORDER BY COUNT_IF(merchant = 'Amazon') DESC) AS amazon_events_percent_rank
FROM islands
GROUP BY DATE_TRUNC('YEAR', txn_time), CASE WHEN burst_id >= 7 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN new_group_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END
HAVING debit_rows > 1;

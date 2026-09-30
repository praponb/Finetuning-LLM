WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(method, 'N/A') AS method_filled
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-07-31 06:00:00')
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    SUM(new_group_flag) FILTER (WHERE method = 'cash') AS cash_total_new_group_flag,
    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_rows,
    PERCENTILE_APPROX(new_group_flag, 0.5) AS new_group_flag_p50,
    PERCENTILE_APPROX(session_no, 0.5) AS session_no_p50,
    COUNT(*) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_rows,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min
FROM sessions_payments
GROUP BY week_start
ORDER BY bank_transfer_rows DESC NULLS FIRST;

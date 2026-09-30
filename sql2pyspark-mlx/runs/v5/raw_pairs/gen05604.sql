WITH gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    SUM(amount) FILTER (WHERE status = 'SUCCESS') AS success_total_amount,
    COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count,
    COUNT(CASE WHEN method = 'cash' THEN 1 END) AS cash_count,
    GROUPING(DATE_TRUNC('MONTH', paid_at)) AS grouping_level
FROM sessionized
GROUP BY CUBE (DATE_TRUNC('MONTH', paid_at), DATE_TRUNC('QUARTER', paid_at))
HAVING SUM(amount) FILTER (WHERE status = 'SUCCESS') < 1000000

WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        CASE WHEN status IN ('FAILED', 'REFUNDED') THEN 'Priority' ELSE 'Other' END AS status_group
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-11-11 18:00:00'
      AND HOUR(paid_at) BETWEEN 12 AND 19
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Grouped metrics
SELECT
    order_id,
    PERCENTILE_APPROX(session_no, 0.9) AS session_no_p90,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_rows,
    MIN(is_new_burst) AS min_is_new_burst,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS success_count,
    ROUND(AVG(is_new_burst), 2) AS avg_is_new_burst,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes
FROM sessionized
GROUP BY 1

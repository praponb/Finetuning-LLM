WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    island_id,
    SIZE(COLLECT_SET(status)) AS num_status,
    MIN(paid_at) AS first_paid_at,
    APPROX_PERCENTILE(island_id, 0.9) AS island_id_p90,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN status = 'FAILED' THEN 1 END), 0), 2) AS amount_per_failed,
    ROUND(AVG(amount), 2) AS avg_amount
FROM sessionized
GROUP BY island_id
ORDER BY island_id NULLS FIRST

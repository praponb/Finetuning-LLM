WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
)
-- Running sum of the flag = session number
SELECT
    id,
    order_id,
    method,
    paid_at,
    status,
    SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
FROM with_gaps;

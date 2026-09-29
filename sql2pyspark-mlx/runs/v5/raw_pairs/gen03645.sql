WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.started_at,
    s.country,
    s.prev_started_at,
    s.is_new_burst,
    s.session_no,
    CASE WHEN country RLIKE '^(UK|JP)$' THEN 'CORE' ELSE 'GROUP_A' END AS country_zone,
    (SELECT MIN(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS min_order_amount
FROM sessionized s
ORDER BY id NULLS LAST;

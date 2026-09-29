WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    amount,
    paid_at,
    status,
    session_id,
    CASE WHEN status RLIKE '^(REFUNDED|SUCCESS)$' THEN 'EXTENDED' WHEN status RLIKE '^(FAILED)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS status_zone
FROM islands
ORDER BY paid_at DESC NULLS FIRST;

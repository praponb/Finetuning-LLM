WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN amount >= 1300 THEN 'High' WHEN amount >= 240 THEN 'Medium' ELSE 'Low' END AS amount_tier,
        COALESCE(method, 'NONE') AS method_filled
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-05-14 17:00:00'
      AND paid_at >= '2024-03-13'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method,
        paid_at,
        method_filled,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_id, method), 4) AS amount_share,
        LAST_VALUE(amount, TRUE) OVER (PARTITION BY order_id ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount
    FROM base
    WHERE order_id = 12
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_windows
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        amount_share,
        burst_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY is_new_burst DESC NULLS LAST, id ASC) AS dedup_rank
        FROM islands
    ) ranked
    WHERE dedup_rank = 1
)
SELECT
    id,
    order_id,
    amount_share,
    burst_id
FROM latest;

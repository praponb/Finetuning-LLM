WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(method)) AS method_title,
        CASE WHEN amount >= 1540 THEN 'High' WHEN amount >= 320 THEN 'Medium' ELSE 'Low' END AS amount_level
    FROM payments
    WHERE method IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        amount,
        paid_at,
        status,
        CUME_DIST() OVER (PARTITION BY status, order_id ORDER BY amount ASC NULLS LAST) AS amount_cume_dist,
        LEAD(amount, 2) OVER (ORDER BY amount ASC, id ASC) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) AS seq_no
    FROM filtered
    WHERE paid_at < '2025-04-01'
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) AS prev_paid_at
    FROM enriched
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        amount,
        paid_at,
        amount_cume_dist,
        next_amount,
        seq_no,
        new_group_flag,
        session_no
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY seq_no ASC NULLS FIRST, id ASC) = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN amount >= 1500 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
    MIN(session_no) AS min_session_no,
    ROUND(AVG(amount), 2) AS avg_amount
FROM latest
GROUP BY CASE WHEN amount >= 1500 THEN 'HIGH' ELSE 'LOW' END

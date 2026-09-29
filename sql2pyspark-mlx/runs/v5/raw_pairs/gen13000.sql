WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at
    FROM payments
    WHERE amount > 1440
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.discount,
        d.shipping_fee
    FROM filtered e
    INNER JOIN orders d
        ON e.order_id = d.id
    WHERE d.channel IN ('web', 'mobile', 'partner')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM joined
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.method,
        e.paid_at,
        e.discount,
        e.burst_id,
        o.channel
    FROM islands e
    LEFT JOIN orders o
        ON e.order_id = o.id
)
-- Window metrics over several different partitions
SELECT
    *,
    SUM(burst_id) OVER w_running AS last4_sum_burst_id,
    COUNT(burst_id) OVER w_running AS last4_count_burst_id,
    MIN(burst_id) OVER w_running AS last4_min_burst_id,
    ROW_NUMBER() OVER w_running_2 AS seq_no
FROM with_dims
WINDOW w_running AS (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_running_2 AS (PARTITION BY method ORDER BY discount ASC, id ASC);

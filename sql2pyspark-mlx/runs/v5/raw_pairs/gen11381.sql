WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    status,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    STDDEV_SAMP(amount) AS amount_stddev,
    MIN(paid_at) AS first_paid_at,
    STDDEV_SAMP(amount) AS amount_stddev_2,
    MAX(amount) AS max_amount,
    MAX(paid_at) AS last_paid_at
FROM islands
GROUP BY status, DATE_TRUNC('MONTH', paid_at)
HAVING MAX(amount) > 0;

WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(amount AS INT) AS amount_int
    FROM payments
    WHERE method IS NOT NULL
      AND paid_at > TIMESTAMP '2023-11-09 19:00:00'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
matched_keys AS (
    -- Set intersection on keys
    SELECT order_id, id
    FROM islands
    INTERSECT
    SELECT order_id, id
    FROM payments
    WHERE method IN ('card', 'cash')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    JOIN matched_keys k ON s.order_id = k.order_id AND s.id = k.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(status, 1, 3) AS status_prefix,
        session_id * amount AS session_id_times_amount
    FROM kept_rows
    WHERE paid_at >= DATE '2023-09-14'
),
filtered_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        amount,
        paid_at,
        status,
        session_id,
        status_prefix,
        session_id_times_amount,
        LENGTH(status) AS status_length,
        ROUND(session_id_times_amount / amount, 2) AS session_id_times_amount_per_amount
    FROM base
    WHERE paid_at > TIMESTAMP '2023-09-17 08:00:00'
      AND order_id IS NOT NULL
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN amount >= 1340 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
        DATE_TRUNC('MONTH', paid_at) AS activity_month,
        COUNT(DISTINCT status) AS distinct_status,
        COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_rows,
        STDDEV_SAMP(amount) AS amount_stddev,
        COUNT_IF(status = 'SUCCESS') AS success_events,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM filtered_payments
    GROUP BY amount_tier, activity_month
)
SELECT
    amount_tier,
    activity_month,
    distinct_status,
    amount_stddev,
    success_events,
    status_list
FROM grouped
ORDER BY success_events, activity_month DESC;

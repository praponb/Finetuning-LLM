WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        CONCAT(status, '/', method) AS status_method
    FROM payments
    WHERE method IS NOT NULL
      AND order_id IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        method,
        paid_at
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC NULLS FIRST, id) = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM deduped
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(session_id) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_session_id,
        MAX(session_id) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_session_id,
        AVG(session_id) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_session_id,
        COUNT(session_id) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_session_id
    FROM islands
    WHERE session_id < 7
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, method
    FROM metrics
    INTERSECT
    SELECT id, method
    FROM payments
    WHERE method IN ('paypal', 'bank_transfer', 'card')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN kept_keys k ON s.id = k.id AND s.method = k.method
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('MONTH', paid_at) AS activity_month,
        session_id,
        APPROX_PERCENTILE(session_start_flag, ARRAY(0.25, 0.5, 0.75)) AS session_start_flag_quartiles,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS row_count,
        GROUPING_ID() AS gid
    FROM verified
    GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', paid_at), session_id), (DATE_TRUNC('MONTH', paid_at)))
)
SELECT
    activity_month,
    session_id,
    session_start_flag_quartiles,
    row_count,
    gid
FROM totals
ORDER BY session_id DESC, activity_month

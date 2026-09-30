WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM payments
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN method IN ('cash', 'promptpay') THEN 'Tier 1' ELSE 'Other' END AS method_group
    FROM sessions_payments
    WHERE method IS NOT NULL
      AND method IN ('promptpay', 'cash')
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged_2
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(is_new_session, TRUE) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_new_session,
        LAST_VALUE(is_new_session) IGNORE NULLS OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_new_session
    FROM islands
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method, status ORDER BY last_known_is_new_session DESC, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_hist AS last_known_is_new_session_dense_rank,
        CUME_DIST() OVER (PARTITION BY status ORDER BY last_known_is_new_session DESC) AS last_known_is_new_session_cume_dist,
        AVG(is_new_burst) OVER (PARTITION BY method, order_id ORDER BY session_id, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS is_new_burst_avg_5,
        LAG(session_id, 2) OVER (PARTITION BY order_id ORDER BY session_id, id) AS prev_session_id,
        LAG(last_known_is_new_session, 1) OVER w_recent AS prev_last_known_is_new_session
    FROM ranked
    WINDOW w_hist AS (PARTITION BY method ORDER BY last_known_is_new_session DESC NULLS LAST), w_recent AS (ORDER BY session_id, id)
),
flagged_3 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst_2
    FROM with_windows
),
islands_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst_2) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged_3
)
-- Aggregate with conditional metrics
SELECT
    status,
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    COUNT(DISTINCT status) AS distinct_status,
    ROUND(AVG(first_known_is_new_session), 2) AS avg_first_known_is_new_session,
    COUNT(*) FILTER (WHERE status = 'REFUNDED') AS refunded_rows,
    COUNT(CASE WHEN method = 'cash' THEN 1 END) AS cash_count
FROM islands_payments
GROUP BY 1, 2;

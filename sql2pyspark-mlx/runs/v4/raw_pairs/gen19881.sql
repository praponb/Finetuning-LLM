WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        CUME_DIST() OVER (ORDER BY amount) AS amount_cume_dist,
        PERCENT_RANK() OVER w_grp AS amount_percent_rank,
        AVG(amount) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior
    FROM payments
    WINDOW w_grp AS (PARTITION BY status ORDER BY amount)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(amount_cume_dist) OVER (PARTITION BY order_id, status ORDER BY amount_percent_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_cume_dist_avg_3_prior,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY amount DESC) AS amount_cume_dist_2,
        AVG(amount_avg_2_prior) OVER (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_avg_2_prior_avg_90d
    FROM metrics
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM enriched
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(amount_cume_dist AS INT) AS amount_cume_dist_int,
        CEIL(session_no) AS session_no_whole
    FROM sessions_payments
    WHERE paid_at > TIMESTAMP '2024-02-08 20:00:00'
      AND method IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, amount_avg_2_prior_avg_90d + session_no AS amount_avg_2_prior_avg_90d_plus_session_no
    FROM base
    WHERE method <> 'card'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM valid
),
flagged_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag_2
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag_2) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged_2
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(session_no_whole) OVER w_recent AS cumulative_session_no_whole,
        AVG(amount_cume_dist_2) OVER w_running AS amount_cume_dist_2_avg_6
    FROM islands
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (ORDER BY amount_cume_dist_avg_3_prior, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    CASE WHEN island_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS island_id_tier,
    PERCENTILE_APPROX(island_id, 0.9) AS island_id_p90,
    MIN(paid_at) AS started_at,
    APPROX_PERCENTILE(amount_avg_2_prior_avg_90d_plus_session_no, ARRAY(0.25, 0.5, 0.75)) AS amount_avg_2_prior_avg_90d_plus_session_no_quartiles,
    PERCENTILE_APPROX(session_start_flag, ARRAY(0.25, 0.5, 0.75)) AS session_start_flag_quartiles
FROM with_windows
GROUP BY DATE_TRUNC('WEEK', paid_at), CASE WHEN island_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING island_id_p90 > 0
ORDER BY island_id_tier DESC

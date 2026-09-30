WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        paid_at,
        status,
        new_group_flag,
        burst_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) AS rank_in_group
        FROM islands
    ) ranked
    WHERE rank_in_group = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY burst_id, id) = 1
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_paid_at
    FROM latest
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        paid_at,
        status,
        new_group_flag,
        burst_id,
        is_new_session,
        session_no,
        PERCENT_RANK() OVER (ORDER BY new_group_flag DESC NULLS FIRST) new_group_flag_percent_rank,
        LAST_VALUE(new_group_flag, TRUE) OVER (ORDER BY is_new_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_new_group_flag,
        SUM(session_no) OVER (ORDER BY prev_paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_session_no,
        SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_new_group_flag
    FROM islands_payments
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        burst_id,
        session_no,
        last_known_new_group_flag,
        cumulative_session_no,
        cumulative_new_group_flag,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY is_new_session DESC, id) event_seq,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY burst_id DESC, id) event_seq_2
    FROM metrics
),
metrics_payments AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(session_no) OVER (PARTITION BY status) status_sum_session_no,
        LEAD(cumulative_new_group_flag, 1) OVER (ORDER BY event_seq_2, id) next_cumulative_new_group_flag,
        RANK() OVER (PARTITION BY status ORDER BY session_no) session_no_rank,
        LEAD(last_known_new_group_flag, 1) OVER (ORDER BY burst_id, id) next_last_known_new_group_flag,
        AVG(cumulative_session_no) OVER (ORDER BY cumulative_new_group_flag, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) cumulative_session_no_avg_3_prior
    FROM with_windows
)
-- Multi-level aggregation
SELECT
    CASE WHEN event_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    session_no,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_rows,
    COUNT_IF(status = 'FAILED') AS failed_events,
    COUNT_IF(status = 'REFUNDED') AS refunded_events,
    COUNT_IF(status = 'SUCCESS') AS success_events,
    ROUND(AVG(cumulative_session_no_avg_3_prior), 2) AS avg_cumulative_session_no_avg_3_prior,
    COUNT(*) AS event_count
FROM metrics_payments
GROUP BY CASE WHEN event_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END, session_no

WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
joined AS (
    -- Attach sessions attributes
    SELECT a.*, s.device
    FROM sessions_web_events a
    LEFT JOIN sessions s
        ON a.session_id = s.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(is_new_session) OVER (ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS is_new_session_avg_2,
        SUM(duration_sec) OVER (ORDER BY is_new_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        MIN(session_no) OVER (PARTITION BY event_type, session_id) AS event_type_min_session_no,
        NTILE(4) OVER (PARTITION BY session_id, event_type ORDER BY is_new_session, id) AS is_new_session_bucket
    FROM joined
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        is_new_session,
        session_no,
        device,
        is_new_session_avg_2,
        running_duration_sec,
        event_type_min_session_no,
        is_new_session_bucket,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        AVG(duration_sec) OVER (PARTITION BY session_id, event_type ORDER BY running_duration_sec, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS duration_sec_avg_5,
        AVG(event_type_min_session_no) OVER (PARTITION BY page ORDER BY is_new_session_bucket, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS event_type_min_session_no_avg_5,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY duration_sec, id) AS seq_no,
        AVG(event_type_min_session_no) OVER w_recent AS event_type_min_session_no_avg_2_prior
    FROM with_windows
    WINDOW w_ordered AS (ORDER BY is_new_session_avg_2 DESC, id), w_recent AS (ORDER BY session_no, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        seq_no,
        CASE WHEN duration_sec >= 380 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
        COUNT(*) FILTER (WHERE device = 'mobile') AS mobile_rows,
        (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
        COUNT_IF(device = 'tablet') AS tablet_events,
        COUNT(*) FILTER (WHERE page = '/checkout') AS checkout_rows,
        ROUND(AVG(seq_no), 2) AS avg_seq_no,
        COUNT(*) AS row_count
    FROM metrics
    GROUP BY seq_no, duration_sec_tier
)
SELECT
    seq_no,
    duration_sec_tier,
    mobile_rows,
    span_minutes,
    checkout_rows,
    avg_seq_no,
    row_count
FROM stats

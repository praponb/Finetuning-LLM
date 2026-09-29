WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_type,
        event_time,
        prev_event_time,
        session_no,
        LAG(is_new_session) OVER w_recent AS prev_is_new_session,
        LEAD(is_new_session) OVER w_recent AS next_is_new_session,
        LAG(session_no) OVER w_recent AS prev_session_no,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM islands
    WHERE page IN ('/cart', '/search')
    WINDOW w_recent AS (PARTITION BY session_id ORDER BY session_no, id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY prev_event_time DESC, id) AS dedup_rank
        FROM scored
    ) x
    WHERE dedup_rank = 1
),
ranked_web_events AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        event_type,
        prev_event_time,
        session_no,
        prev_is_new_session,
        prev_session_no,
        row_seq
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time NULLS FIRST, id) AS latest_rank
        FROM ranked
    ) x
    WHERE latest_rank = 1
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(prev_event_time) OVER (PARTITION BY event_type ORDER BY prev_event_time, id) IS NULL
                 OR prev_event_time > LAG(prev_event_time) OVER (PARTITION BY event_type ORDER BY prev_event_time, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM ranked_web_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY prev_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps_2
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        prev_session_no,
        COUNT(*) AS num_rows,
        MAX(prev_event_time) AS last_seen,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_cart_rows,
        COUNT(*) AS row_count,
        COUNT_IF(event_type = 'purchase') AS purchase_events,
        MIN(prev_event_time) AS first_prev_event_time
    FROM sessions_web_events
    GROUP BY CUBE (prev_session_no)
)
SELECT
    prev_session_no,
    last_seen,
    row_count,
    purchase_events,
    first_prev_event_time
FROM grouped

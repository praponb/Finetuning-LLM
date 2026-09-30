WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_time,
        SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        event_time,
        session_no
    FROM sessionized
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS event_seq,
        ROW_NUMBER() OVER (ORDER BY session_no DESC, id) AS row_seq
    FROM latest
    WINDOW w_grp AS (ORDER BY session_no, id)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(event_seq, 0) AS event_seq_filled
    FROM with_windows
    WHERE event_time < '2025-09-01'
)
SELECT
    id,
    session_no,
    event_seq,
    event_seq_filled
FROM cleaned;

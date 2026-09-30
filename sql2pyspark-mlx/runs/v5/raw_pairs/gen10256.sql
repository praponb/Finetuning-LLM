WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        LENGTH(page) AS page_length
    FROM web_events
    WHERE event_time > TO_DATE('2023-08-10')
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC) AS prev_event_time
    FROM cleaned
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        prev_event_time,
        session_id_2,
        AVG(page_length) OVER w_hist AS page_length_avg_3,
        NTILE(5) OVER (PARTITION BY event_type ORDER BY session_start_flag ASC, id ASC) AS session_start_flag_bucket
    FROM sessions_web_events
    WINDOW w_hist AS (ORDER BY prev_event_time ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(session_id_2) OVER (PARTITION BY session_id ORDER BY session_id_2 ASC, id ASC) AS next_session_id_2,
        LAG(session_id_2) OVER (PARTITION BY session_id ORDER BY session_id_2 ASC, id ASC) AS prev_session_id_2,
        LEAD(page_length_avg_3) OVER (PARTITION BY session_id ORDER BY session_id_2 ASC, id ASC) AS next_page_length_avg_3,
        LAG(page_length_avg_3) OVER (PARTITION BY session_id ORDER BY session_id_2 ASC, id ASC) AS prev_page_length_avg_3,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY session_id_2 ASC, id ASC) AS seq_no,
        LAG(session_id_2, 1) OVER (ORDER BY page_length_avg_3 ASC, id ASC) AS prev_session_id_2_2
    FROM scored
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(prev_event_time) OVER (PARTITION BY session_id ORDER BY prev_event_time ASC, id ASC) IS NULL
                 OR prev_event_time > LAG(prev_event_time) OVER (PARTITION BY session_id ORDER BY prev_event_time ASC, id ASC) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_windows
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY prev_event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.next_session_id_2,
        s.prev_session_id_2,
        s.prev_page_length_avg_3,
        s.seq_no,
        p2.browser
    FROM sessionized s
    LEFT JOIN sessions p2
        ON s.session_id = p2.id
    WHERE p2.device != 'mobile'
)
SELECT
    s.id,
    s.session_id,
    s.prev_session_id_2,
    s.prev_page_length_avg_3,
    s.seq_no,
    s.browser,
    (SELECT AVG(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS avg_web_event_duration_sec
FROM enriched s;

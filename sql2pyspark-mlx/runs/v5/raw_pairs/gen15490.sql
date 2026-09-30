WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessionized
),
sessionized_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged_2
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        page,
        event_time,
        session_id_2,
        MAX(session_id_2) OVER w_grp AS last7_max_session_id_2,
        AVG(session_id_2) OVER w_grp AS last7_avg_session_id_2,
        COUNT(session_id_2) OVER w_grp AS last7_count_session_id_2,
        AVG(new_group_flag) OVER (ORDER BY new_group_flag, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS new_group_flag_avg_3_prior
    FROM sessionized_web_events
    WHERE island_id < 2
    WINDOW w_grp AS (PARTITION BY page ORDER BY session_id_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM metrics
),
sessionized_web_events_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_3
    FROM gaps
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        b.country,
        b.device
    FROM sessionized_web_events_2 src
    INNER JOIN sessions b
        ON src.session_id = b.id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.page,
        src.event_time,
        src.last7_avg_session_id_2,
        src.last7_count_session_id_2,
        src.new_group_flag_avg_3_prior,
        src.session_start_flag,
        src.device,
        p.device AS session_device,
        p.browser
    FROM joined src
    INNER JOIN sessions p
        ON src.session_id = p.id
),
with_prev_2 AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY device ORDER BY event_time, id) AS prev_event_time
    FROM with_dims
),
gaps_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev_2
),
sessionized_web_events_3 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps_2
)
SELECT
    id,
    session_id,
    page,
    last7_avg_session_id_2,
    last7_count_session_id_2,
    new_group_flag_avg_3_prior,
    session_start_flag,
    device,
    browser
FROM sessionized_web_events_3
ORDER BY device DESC;

WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.event_time,
        e.duration_sec,
        e.session_id_2,
        d.browser,
        d.device
    FROM islands e
    LEFT JOIN sessions d
        ON e.session_id = d.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(session_id_2) OVER (ORDER BY session_id_2, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS session_id_2_avg_5_prior,
        AVG(duration_sec) OVER (PARTITION BY browser ORDER BY session_id_2, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_5_prior,
        FIRST_VALUE(device) IGNORE NULLS OVER (PARTITION BY device, browser ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_device,
        ROW_NUMBER() OVER (ORDER BY event_time DESC, id) AS seq_no,
        SUM(session_id_2) OVER (PARTITION BY device, session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_session_id_2
    FROM with_dims
    WHERE device = 'mobile'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(duration_sec) OVER w_running AS last7_avg_duration_sec,
        SUM(duration_sec) OVER w_running AS last7_sum_duration_sec,
        MAX(duration_sec) OVER w_running AS last7_max_duration_sec,
        NTILE(10) OVER (PARTITION BY browser ORDER BY duration_sec_avg_5_prior DESC, id) AS duration_sec_avg_5_prior_bucket
    FROM metrics
    WINDOW w_running AS (PARTITION BY browser ORDER BY session_id_2_avg_5_prior, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device ORDER BY last7_avg_duration_sec, id) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(duration_sec_avg_5_prior) OVER w_part AS prev_duration_sec_avg_5_prior,
        LAG(last7_avg_duration_sec) OVER w_part AS prev_last7_avg_duration_sec,
        LEAD(last7_avg_duration_sec) OVER w_part AS next_last7_avg_duration_sec,
        ROW_NUMBER() OVER w_part AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY running_session_id_2 DESC, id) AS row_seq
    FROM latest
    WINDOW w_part AS (PARTITION BY device ORDER BY duration_sec, id)
),
with_dims_web_events AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        p.country,
        p.device AS session_device
    FROM enriched src
    LEFT JOIN sessions p
        ON src.session_id = p.id
)
SELECT
    s.id,
    s.session_id,
    s.event_time,
    s.duration_sec,
    s.session_id_2,
    s.browser,
    s.device,
    s.session_id_2_avg_5_prior,
    s.duration_sec_avg_5_prior,
    CASE WHEN device RLIKE '^(desktop|mobile)$' THEN 'GROUP_A' WHEN device RLIKE '^(tablet)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS device_zone,
    (SELECT MAX(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS max_web_event_duration_sec
FROM with_dims_web_events s
ORDER BY device DESC, duration_sec DESC

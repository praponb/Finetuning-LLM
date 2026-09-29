WITH joined AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        d.device,
        d.browser,
        d.country
    FROM web_events s
    INNER JOIN sessions d
        ON s.session_id = d.id
    WHERE d.country = 'SG'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY event_type ORDER BY event_time DESC NULLS LAST) AS rank_in_group
        FROM joined
    ) t
    WHERE rank_in_group = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(duration_sec, TRUE) OVER (PARTITION BY browser, event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        MIN(duration_sec) OVER (PARTITION BY device) AS device_min_duration_sec,
        NTILE(3) OVER (PARTITION BY country ORDER BY duration_sec DESC, id) AS duration_sec_bucket
    FROM latest
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY device ORDER BY event_time, id) AS prev_event_time
    FROM metrics
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        device,
        browser,
        country,
        rank_in_group,
        device_min_duration_sec,
        duration_sec_bucket,
        SUM(new_group_flag) OVER (PARTITION BY device ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        device,
        browser,
        rank_in_group,
        device_min_duration_sec,
        LEAD(duration_sec_bucket, 1, 0) OVER w_part AS next_duration_sec_bucket,
        ROUND(duration_sec_bucket / SUM(duration_sec_bucket) OVER (PARTITION BY device, browser), 4) AS duration_sec_bucket_share,
        SUM(session_no) OVER w_part_2 AS cumulative_session_no
    FROM sessions_web_events
    WINDOW w_part AS (ORDER BY session_no, id), w_part_2 AS (PARTITION BY event_type ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    device,
    browser,
    rank_in_group
FROM with_windows;

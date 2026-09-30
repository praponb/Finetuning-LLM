WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        p.country,
        p.browser,
        p.device
    FROM sessions_web_events src
    LEFT JOIN sessions p
        ON src.session_id = p.id
    WHERE p.device IN ('tablet', 'mobile')
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, page
    FROM joined
    EXCEPT DISTINCT
    SELECT id, page
    FROM web_events
    WHERE page IN ('/cart', '/product')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN kept_keys k ON s.id = k.id AND s.page = k.page
),
metrics AS (
    -- Ranking and gap features
    SELECT *, COUNT(new_group_flag) OVER (PARTITION BY browser ORDER BY CAST(prev_event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS new_group_flag_count_7d
    FROM verified
),
joined_web_events AS (
    -- Attach sessions attributes
    SELECT
        src.session_id,
        src.event_type,
        src.event_time,
        src.prev_event_time,
        src.new_group_flag,
        src.session_no,
        src.country,
        src.browser,
        src.new_group_flag_count_7d,
        b.browser AS session_browser
    FROM metrics src
    LEFT JOIN sessions b
        ON src.session_id = b.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        session_id,
        event_type,
        event_time,
        new_group_flag_count_7d,
        SUM(new_group_flag_count_7d) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS new_group_flag_count_7d_sum_30d
    FROM joined_web_events
)
SELECT
    s.session_id,
    s.event_time,
    s.new_group_flag_count_7d,
    s.new_group_flag_count_7d_sum_30d,
    (SELECT SUM(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS sum_web_event_duration_sec
FROM enriched s
ORDER BY new_group_flag_count_7d DESC NULLS FIRST;

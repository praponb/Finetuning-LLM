WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.event_time,
        e.duration_sec,
        s.country,
        s.browser
    FROM web_events e
    LEFT JOIN sessions s
        ON e.session_id = s.id
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.event_time,
        b.country AS session_country,
        b.device
    FROM enriched src
    INNER JOIN sessions b
        ON src.session_id = b.id
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_country ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_country ORDER BY event_time, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM joined
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_country ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_country ORDER BY event_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM islands
),
islands_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS session_start_flag_tier,
        CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_burst_tier,
        ROUND(SUM(is_new_burst) / NULLIF(COUNT(CASE WHEN device = 'mobile' THEN 1 END), 0), 2) AS is_new_burst_per_mobile,
        SUM(CASE WHEN session_country = 'SG' THEN is_new_burst ELSE 0 END) AS sg_is_new_burst,
        GROUPING_ID() AS grouping_level
    FROM islands_web_events
    GROUP BY GROUPING SETS ((CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END))
    HAVING sg_is_new_burst < 1000000
)
SELECT
    session_start_flag_tier,
    is_new_burst_per_mobile,
    sg_is_new_burst
FROM daily
ORDER BY sg_is_new_burst;

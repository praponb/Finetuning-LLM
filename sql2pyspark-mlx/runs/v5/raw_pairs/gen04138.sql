WITH with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
flagged AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessions_web_events AS (
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
kept_keys AS (
    SELECT event_type, session_id
    FROM sessions_web_events
    EXCEPT
    SELECT event_type, session_id
    FROM web_events
    WHERE duration_sec >= 258
),
kept_rows AS (
    SELECT s.*
    FROM sessions_web_events s
    INNER JOIN kept_keys k ON s.event_type = k.event_type AND s.session_id = k.session_id
),
with_dims AS (
    SELECT
        s.session_id,
        s.event_type,
        s.event_time,
        p2.country,
        p2.device,
        p2.browser
    FROM kept_rows s
    LEFT JOIN sessions p2
        ON s.session_id = p2.id
    WHERE p2.browser NOT IN ('Safari', 'Chrome', 'Edge')
),
filtered AS (
    SELECT
        session_id,
        event_type,
        event_time,
        country,
        device,
        browser,
        CONCAT(country, ' ', device) AS country_device,
        INITCAP(TRIM(event_type)) AS event_type_title
    FROM with_dims
    WHERE event_type = 'click'
)
SELECT
    s.session_id,
    s.event_type,
    s.event_time,
    s.country,
    s.device,
    s.browser,
    s.country_device,
    s.event_type_title,
    (SELECT SUM(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS sum_web_event_duration_sec
FROM filtered s

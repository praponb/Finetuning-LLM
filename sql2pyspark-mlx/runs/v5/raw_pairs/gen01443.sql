WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        ROUND(duration_sec * 100, 2) AS adjusted_duration_sec
    FROM web_events
    WHERE event_time > TO_DATE('2023-05-11')
      AND page = '/cart'
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p.country,
        p.browser
    FROM valid s
    LEFT JOIN sessions p
        ON s.session_id = p.id
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_dims
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        country,
        browser,
        SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        country,
        session_id_2,
        LENGTH(browser) AS browser_length,
        HOUR(event_time) AS event_time_hour
    FROM sessionized
    WHERE browser IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        session_id,
        country,
        session_id_2
    FROM filtered
) src
PIVOT (
    MAX(session_id_2) AS total,
    COUNT(1) AS cnt
    FOR country IN ('SG', 'US')
);

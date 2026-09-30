WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        DATE_TRUNC('month', started_at) AS started_at_period,
        CONCAT_WS('/', device, country) AS device_country_key,
        COALESCE(browser, 'NONE') AS browser_filled
    FROM sessions
    WHERE browser IS DISTINCT FROM 'Chrome'
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.web_event_duration_sec
    FROM valid s
    JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        started_at_period,
        device_country_key,
        browser_filled,
        web_event_duration_sec,
        CASE WHEN device IN ('tablet', 'desktop') THEN 'Priority' ELSE 'Other' END AS device_group
    FROM with_last
    WHERE web_event_duration_sec = 286
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at_period) OVER (PARTITION BY browser ORDER BY started_at_period, id) IS NULL
                 OR started_at_period > LAG(started_at_period) OVER (PARTITION BY browser ORDER BY started_at_period, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY browser ORDER BY started_at_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
kept_keys AS (
    -- Set difference on keys
    SELECT country, browser
    FROM islands
    EXCEPT
    SELECT country, browser
    FROM sessions
    WHERE device NOT IN ('mobile', 'desktop')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    INNER JOIN kept_keys k ON s.country = k.country AND s.browser = k.browser
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        browser,
        started_at,
        started_at_period,
        device_country_key,
        web_event_duration_sec,
        device_group,
        is_new_session,
        session_no,
        COALESCE(country, 'N/A') AS country_filled,
        LENGTH(browser) AS browser_length,
        COALESCE(country, 'NONE') AS country_clean
    FROM verified
    WHERE session_no = 8
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < s.started_at
        )
)
-- Grouped metrics
SELECT
    device,
    DATE_TRUNC('QUARTER', started_at_period) AS quarter_start,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    COUNT(DISTINCT device) AS distinct_devices
FROM not_flagged
GROUP BY GROUPING SETS ((device, DATE_TRUNC('QUARTER', started_at_period)), (device), ())

WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(device) device_length,
        COALESCE(browser, 'N/A') resolved_browser
    FROM sessions
    WHERE started_at >= TO_TIMESTAMP('2023-07-29 09:00:00')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM sessions_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < s.started_at
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, browser
    FROM eligible
    EXCEPT
    SELECT id, browser
    FROM sessions
    WHERE country IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN matched_keys k ON s.id = k.id AND s.browser = k.browser
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < s.started_at
        )
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count,
    COUNT_IF(browser = 'Chrome') AS chrome_events,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SIZE(COLLECT_SET(browser)) AS num_browsers
FROM kept
GROUP BY DATE_TRUNC('QUARTER', started_at)
HAVING COUNT_IF(browser = 'Chrome') < 1;

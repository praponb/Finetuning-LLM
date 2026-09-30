WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(browser) AS browser_length,
        COALESCE(country, 'N/A') AS country_clean
    FROM sessions
    WHERE started_at >= DATE '2023-07-13'
      AND browser IS NOT NULL
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) AS prev_started_at
    FROM cleaned
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    browser,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    MAX(is_new_burst) AS max_is_new_burst,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    PERCENTILE_APPROX(browser_length, ARRAY(0.25, 0.5, 0.75)) AS browser_length_quartiles
FROM sessions_sessions
GROUP BY browser;

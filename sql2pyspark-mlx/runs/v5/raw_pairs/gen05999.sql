WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(country) AS country_lower,
        COALESCE(country, 'UNKNOWN') AS resolved_country
    FROM sessions
    WHERE started_at >= TO_DATE('2023-03-26')
      AND country IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    browser,
    MIN(started_at) AS started_at,
    AVG(island_id) FILTER (WHERE device = 'desktop') AS desktop_avg_island_id
FROM islands
GROUP BY browser
HAVING AVG(island_id) FILTER (WHERE device = 'desktop') > 0
ORDER BY browser DESC NULLS LAST;

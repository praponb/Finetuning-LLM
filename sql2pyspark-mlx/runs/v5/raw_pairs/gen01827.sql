WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        browser,
        started_at,
        country,
        LOWER(browser) AS browser_lower,
        CASE WHEN browser = 'Safari' THEN 1 ELSE 0 END AS is_safari,
        COALESCE(country, 'OTHER') AS resolved_country
    FROM sessions
    WHERE started_at >= DATE '2023-03-19'
      AND country IS DISTINCT FROM 'DE'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    country,
    APPROX_PERCENTILE(is_safari, 0.95) AS is_safari_p95,
    SUM(CASE WHEN country = 'JP' THEN burst_id ELSE 0 END) AS jp_burst_id,
    COUNT_IF(country = 'DE') AS de_events,
    COUNT(*) AS row_count
FROM islands
GROUP BY 1
HAVING jp_burst_id > 0
ORDER BY row_count DESC NULLS FIRST;

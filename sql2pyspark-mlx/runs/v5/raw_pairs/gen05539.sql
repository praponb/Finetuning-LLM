WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        CASE WHEN country IN ('US', 'DE') THEN 'Tier 1' ELSE 'Other' END AS country_group
    FROM sessions
    WHERE started_at >= DATE '2023-03-23'
      AND customer_id IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    country,
    customer_id,
    COUNT(DISTINCT country) AS distinct_countries,
    SUM(CASE WHEN device = 'tablet' THEN session_id ELSE 0 END) AS tablet_session_id,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    COUNT_IF(device = 'desktop') AS desktop_events,
    MAX(session_id) AS max_session_id,
    SUM(CASE WHEN country = 'TH' THEN session_id ELSE 0 END) AS th_session_id
FROM islands
GROUP BY country, customer_id
HAVING COUNT(DISTINCT country) < 1000
ORDER BY desktop_events DESC NULLS LAST;

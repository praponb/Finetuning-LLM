WITH kept_keys AS (
    -- Set difference on keys
    SELECT country
    FROM sessions
    EXCEPT ALL
    SELECT country
    FROM sessions
    WHERE customer_id IN (13, 23, 29, 36)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions s
    JOIN kept_keys k ON s.country = k.country
)
-- Aggregate with conditional metrics
SELECT
    browser,
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    COUNT_IF(country = 'DE') AS de_events,
    MAX(started_at) AS ended_at,
    COUNT(*) FILTER (WHERE browser = 'Chrome') AS chrome_rows,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    COUNT(*) AS cnt
FROM kept_rows
GROUP BY browser, quarter_start
HAVING de_events >= 1;

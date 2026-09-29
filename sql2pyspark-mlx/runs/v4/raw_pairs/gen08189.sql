WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        LENGTH(browser) AS browser_length
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-10-30 03:00:00'
      AND browser IS NOT NULL
      AND country NOT IN ('UK', 'JP', 'US')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    APPROX_PERCENTILE(burst_id, ARRAY(0.25, 0.5, 0.75)) AS burst_id_quartiles,
    COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes
FROM sessionized
GROUP BY DATE_TRUNC('YEAR', started_at)
ORDER BY mobile_count DESC NULLS FIRST;

WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', device, country) AS device_country_key,
        COALESCE(browser, 'UNKNOWN') AS resolved_browser
    FROM sessions
    WHERE started_at >= '2023-04-01'
      AND customer_id IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        started_at,
        country,
        device_country_key,
        SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    island_id,
    MIN(started_at) AS started_at,
    ROUND(AVG(island_id), 2) AS avg_island_id
FROM sessionized
GROUP BY DATE_TRUNC('MONTH', started_at), island_id;

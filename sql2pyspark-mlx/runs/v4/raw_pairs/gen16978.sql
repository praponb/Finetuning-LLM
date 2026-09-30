WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        CAST(started_at AS DATE) AS started_at_day,
        COALESCE(browser, 'DIRECT') AS resolved_browser
    FROM sessions
    WHERE started_at >= TIMESTAMP '2024-02-23 12:00:00'
      AND browser = 'Edge'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT device, id
    FROM cleaned
    INTERSECT
    SELECT device, id
    FROM sessions
    WHERE TO_DATE(started_at) >= '2023-04-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.device = k.device AND s.id = k.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
),
grouped AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('QUARTER', started_at_day) AS quarter_start,
        DATE_TRUNC('YEAR', started_at) AS year_start,
        country,
        COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
        COUNT(DISTINCT browser) AS distinct_browsers,
        COUNT_IF(browser = 'Edge') AS edge_events,
        SIZE(COLLECT_SET(device)) AS num_devices,
        COUNT(DISTINCT country) AS distinct_countries
    FROM eligible
    GROUP BY DATE_TRUNC('QUARTER', started_at_day), DATE_TRUNC('YEAR', started_at), country
)
SELECT
    quarter_start,
    year_start,
    country,
    th_count,
    distinct_browsers,
    distinct_countries
FROM grouped;

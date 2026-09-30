WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(device)) device_title,
        COALESCE(browser, 'UNKNOWN') resolved_browser
    FROM sessions
    WHERE started_at >= DATE '2023-06-16'
      AND customer_id IS NOT NULL
      AND device != 'desktop'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    CASE WHEN island_id >= 6 THEN 'LARGE' ELSE 'SMALL' END AS island_id_tier,
    browser,
    SIZE(COLLECT_SET(device)) AS num_devices,
    AVG(island_id) AS avg_island_id,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    MIN(island_id) AS min_island_id
FROM sessions_sessions
GROUP BY CASE WHEN island_id >= 6 THEN 'LARGE' ELSE 'SMALL' END, browser;

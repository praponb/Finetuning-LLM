WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(browser, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS row_seq,
        LAST_VALUE(country) IGNORE NULLS OVER w_grp AS last_known_country,
        ROW_NUMBER() OVER (ORDER BY started_at, id) AS row_seq_2,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS seq_no
    FROM sessions
    WINDOW w_grp AS (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    COUNT_IF(device = 'mobile') AS mobile_events,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    COUNT(CASE WHEN device = 'desktop' THEN 1 END) AS desktop_count,
    COUNT_IF(browser = 'Safari') AS safari_events,
    COUNT_IF(country = 'SG') AS sg_events
FROM metrics
GROUP BY year_start
ORDER BY num_browsers NULLS FIRST;

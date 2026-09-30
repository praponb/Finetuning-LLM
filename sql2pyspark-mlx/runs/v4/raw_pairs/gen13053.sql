WITH filtered AS (
    SELECT
        *,
        COALESCE(browser, 'Unknown') AS browser_filled,
        CASE WHEN browser IN ('Edge', 'Chrome') THEN 'Group A' ELSE 'Other' END AS browser_group
    FROM sessions
    WHERE started_at > DATE '2024-02-19'
      AND started_at >= '2024-04-01'
)
SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    browser,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
    MAX(started_at) AS ended_at,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT_IF(browser = 'Edge') AS edge_events,
    COUNT(*) AS cnt,
    GROUPING(DATE_TRUNC('WEEK', started_at)) AS grouping_level
FROM filtered
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', started_at), browser), (DATE_TRUNC('WEEK', started_at)), ())

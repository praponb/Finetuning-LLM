WITH base AS (
    SELECT
        id,
        started_at,
        country,
        SUBSTRING(country, 1, 3) AS country_prefix,
        COALESCE(browser, 'N/A') AS resolved_browser
    FROM sessions
    WHERE customer_id IS NOT NULL
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    CASE WHEN session_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS session_id_tier,
    DATE_TRUNC('WEEK', started_at) AS week_start,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    COUNT(DISTINCT country) AS distinct_countries,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    APPROX_PERCENTILE(session_id, 0.5) AS session_id_p50,
    COUNT(DISTINCT country) AS distinct_countries_2,
    GROUPING_ID() AS grouping_level
FROM sessionized
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', started_at), CASE WHEN session_id >= 8 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', started_at)), (DATE_TRUNC('QUARTER', started_at), CASE WHEN session_id >= 8 THEN 'LARGE' ELSE 'SMALL' END), ())
ORDER BY session_id_tier DESC NULLS LAST

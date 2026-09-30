WITH with_dims AS (
    SELECT
        src.*,
        p.country,
        p.device,
        p.browser
    FROM web_events src
    LEFT JOIN sessions p
        ON src.session_id = p.id
)
SELECT
    CASE WHEN duration_sec >= 470 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    DATE_TRUNC('WEEK', event_time) AS week_start,
    device,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SUM(CASE WHEN browser = 'Safari' THEN duration_sec ELSE 0 END) AS safari_duration_sec,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min,
    GROUPING(CASE WHEN duration_sec >= 470 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM with_dims
GROUP BY ROLLUP (CASE WHEN duration_sec >= 470 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', event_time), device)
ORDER BY duration_sec_tier NULLS LAST;

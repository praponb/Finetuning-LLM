SELECT
    CASE WHEN duration_sec >= 415 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    MIN(event_time) AS started_at,
    APPROX_PERCENTILE(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    PERCENTILE_APPROX(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles_2,
    SIZE(COLLECT_SET(page)) AS num_pages
FROM web_events
GROUP BY 1
ORDER BY duration_sec_tier;

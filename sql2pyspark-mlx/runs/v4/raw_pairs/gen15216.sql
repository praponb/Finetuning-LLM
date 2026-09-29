SELECT
    CASE WHEN duration_sec >= 536 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    MAX(duration_sec) AS max_duration_sec,
    PERCENTILE_APPROX(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    PERCENTILE_APPROX(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles_2,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes
FROM web_events
GROUP BY CUBE (CASE WHEN duration_sec >= 536 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 > 0
ORDER BY duration_sec_tier DESC NULLS LAST;

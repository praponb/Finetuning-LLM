SELECT
    id,
    duration_sec,
    LAG(duration_sec, 1) OVER (PARTITION BY page ORDER BY event_time, id) prev_duration_sec,
    AVG(duration_sec) OVER (ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) duration_sec_avg_3_prior,
    NTILE(4) OVER (PARTITION BY page ORDER BY duration_sec DESC, id) duration_sec_bucket,
    NTILE(10) OVER (PARTITION BY page ORDER BY duration_sec DESC, id) duration_sec_bucket_2
FROM web_events

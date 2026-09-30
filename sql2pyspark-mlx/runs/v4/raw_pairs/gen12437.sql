SELECT
    CASE WHEN duration_sec >= 407 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    APPROX_PERCENTILE(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    MIN(event_time) AS first_seen,
    BIT_AND(duration_sec) AS duration_sec_bit_and
FROM web_events
GROUP BY CASE WHEN duration_sec >= 407 THEN 'HIGH' ELSE 'LOW' END
HAVING duration_sec_bit_and >= 1
ORDER BY duration_sec_bit_and

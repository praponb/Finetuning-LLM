SELECT
    CASE WHEN duration_sec >= 384 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    page,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    BIT_OR(duration_sec) AS duration_sec_bit_or
FROM web_events
GROUP BY GROUPING SETS ((CASE WHEN duration_sec >= 384 THEN 'PREMIUM' ELSE 'STANDARD' END, page), (CASE WHEN duration_sec >= 384 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
ORDER BY duration_sec_bit_or DESC;

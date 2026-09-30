WITH deduped AS (
    SELECT *
    FROM device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits, id) = 1
)
SELECT
    CASE WHEN temp_c >= 40.4 THEN 'PREMIUM' ELSE 'STANDARD' END AS temp_c_tier,
    PERCENTILE_APPROX(temp_c, 0.9) AS temp_c_p90,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS duration_min,
    CORR(status_bits, pressure) AS corr_status_bits_pressure,
    COUNT(*) AS event_count,
    MIN(reading_ts) AS first_seen
FROM deduped
GROUP BY CASE WHEN temp_c >= 40.4 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING temp_c_p90 < 1000000
ORDER BY duration_min DESC NULLS LAST;

WITH with_gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    CASE WHEN island_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS island_id_tier,
    ROUND(SUM(status_bits) / NULLIF(COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END), 0), 2) AS status_bits_per_dv_01,
    STDDEV_SAMP(humidity) AS humidity_stddev,
    MIN(island_id) AS min_island_id,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes
FROM islands
GROUP BY CUBE (DATE_TRUNC('DAY', reading_ts), CASE WHEN island_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING MIN(island_id) > 0
ORDER BY min_island_id DESC NULLS LAST;

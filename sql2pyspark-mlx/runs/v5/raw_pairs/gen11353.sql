WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        humidity,
        status_bits,
        firmware,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_humidity,
        COUNT(humidity) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_humidity,
        MIN(humidity) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_humidity
    FROM device_readings
)
-- Multi-level aggregation
SELECT
    CASE WHEN humidity >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END AS humidity_tier,
    APPROX_PERCENTILE(humidity, ARRAY(0.25, 0.5, 0.75)) AS humidity_quartiles,
    ROUND(AVG(last3_min_humidity), 2) AS avg_last3_min_humidity,
    COUNT(DISTINCT firmware) AS distinct_firmwares,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM metrics
GROUP BY CASE WHEN humidity >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END

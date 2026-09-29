WITH valid AS (
    SELECT *, ROUND(status_bits, 2) AS status_bits_rounded
    FROM device_readings
    WHERE reading_ts < '2024-03-01'
),
metrics AS (
    SELECT
        id,
        device_code,
        status_bits,
        firmware,
        AVG(status_bits_rounded) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_bits_rounded_avg_30m,
        SUM(temp_c) OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_temp_c,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY status_bits_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_humidity
    FROM valid
),
cleaned AS (
    SELECT *, GREATEST(status_bits, cumulative_temp_c) AS greatest_status_bits_cumulative_temp_c
    FROM metrics
    WHERE status_bits_rounded_avg_30m BETWEEN 44 AND 61
),
metrics_device_readings AS (
    SELECT
        id,
        status_bits,
        firmware,
        status_bits_rounded_avg_30m,
        FIRST_VALUE(cumulative_temp_c) IGNORE NULLS OVER (ORDER BY cumulative_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_temp_c,
        ROW_NUMBER() OVER (ORDER BY cumulative_temp_c, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS event_seq
    FROM cleaned
)
SELECT
    CASE WHEN status_bits >= 13 THEN 'HIGH' ELSE 'LOW' END AS status_bits_tier,
    COUNT(*) AS event_count,
    COUNT(DISTINCT firmware) AS distinct_firmwares,
    APPROX_PERCENTILE(status_bits, 0.9) AS status_bits_p90,
    ROUND(AVG(first_known_cumulative_temp_c), 2) AS avg_first_known_cumulative_temp_c
FROM metrics_device_readings
GROUP BY status_bits_tier
HAVING COUNT(*) < 1000

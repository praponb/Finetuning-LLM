WITH kept_keys AS (
    SELECT id
    FROM device_readings
    EXCEPT
    SELECT id
    FROM device_readings
    WHERE reading_ts < '2025-07-01'
),
kept_rows AS (
    SELECT s.*
    FROM device_readings s
    JOIN kept_keys k ON s.id = k.id
)
SELECT
    device_code,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    MAX(humidity) FILTER (WHERE device_code = 'DV-04') AS dv_04_max_humidity,
    BIT_AND(status_bits) AS status_bits_bit_and,
    MAX(reading_ts) AS ended_at,
    COUNT_IF(device_code = 'DV-03') AS dv_03_events,
    COUNT(*) AS cnt,
    GROUPING_ID() AS grouping_id
FROM kept_rows
GROUP BY ROLLUP (device_code, DATE_TRUNC('DAY', reading_ts))
ORDER BY dv_04_max_humidity DESC;

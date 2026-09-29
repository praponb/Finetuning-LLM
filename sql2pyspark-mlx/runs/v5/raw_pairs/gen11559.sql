WITH base AS (
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        status_bits,
        firmware,
        CASE WHEN firmware = '2.0.1' THEN 1 ELSE 0 END AS is_2_0_1
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
)
SELECT
    id,
    device_code,
    humidity,
    status_bits,
    firmware,
    is_2_0_1,
    LEAD(pressure) OVER w_running AS next_pressure,
    LAG(status_bits) OVER w_running AS prev_status_bits,
    LEAD(status_bits) OVER w_running AS next_status_bits,
    ROW_NUMBER() OVER w_running AS event_seq,
    LAST_VALUE(humidity, TRUE) OVER (ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity
FROM base
WINDOW w_running AS (PARTITION BY device_code ORDER BY is_2_0_1, id);

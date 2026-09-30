WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(firmware) AS firmware_lower
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND pressure >= 1010
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(humidity, 2) OVER (ORDER BY reading_ts, id) AS prev_humidity,
        RANK() OVER (PARTITION BY device_code ORDER BY temp_c DESC NULLS FIRST) AS temp_c_rank,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS pressure_avg_3
    FROM cleaned
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY humidity) AS humidity_percent_rank,
        LAG(pressure, 1) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_pressure,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id) AS event_seq,
        CUME_DIST() OVER (PARTITION BY device_code ORDER BY temp_c) AS temp_c_cume_dist,
        LEAD(pressure, 1) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS next_pressure
    FROM metrics
)
SELECT
    id,
    device_code,
    humidity,
    pressure,
    status_bits,
    firmware,
    firmware_lower,
    prev_humidity,
    temp_c_rank,
    CASE WHEN device_code RLIKE '^(DV-02|DV-03)$' THEN 'EXTENDED' WHEN device_code RLIKE '^(DV-01|DV-04)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS device_code_zone
FROM with_windows;

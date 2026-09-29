WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(firmware, ', ', device_code) AS firmware_device_code
    FROM device_readings
    WHERE device_code IN ('DV-04', 'DV-03')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity NULLS FIRST, id) = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT device_code, firmware
    FROM deduped
    EXCEPT DISTINCT
    SELECT device_code, firmware
    FROM device_readings
    WHERE device_code IN ('DV-02', 'DV-03')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.device_code = k.device_code AND s.firmware = k.firmware
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware_device_code,
        pressure - temp_c AS pressure_minus_temp_c,
        HOUR(reading_ts) AS reading_ts_hour
    FROM reconciled
    WHERE reading_ts > TO_DATE('2025-09-01')
),
scored AS (
    -- Running and moving aggregates
    SELECT *, RANK() OVER (ORDER BY pressure_minus_temp_c DESC) AS pressure_minus_temp_c_rank
    FROM base
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('DAY', reading_ts) AS activity_day,
        MAX(reading_ts_hour) AS max_reading_ts_hour,
        APPROX_PERCENTILE(reading_ts_hour, ARRAY(0.25, 0.5, 0.75)) AS reading_ts_hour_quartiles
    FROM scored
    GROUP BY DATE_TRUNC('DAY', reading_ts)
)
SELECT
    activity_day,
    max_reading_ts_hour,
    reading_ts_hour_quartiles
FROM summary

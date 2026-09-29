WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT firmware
    FROM device_readings
    INTERSECT ALL
    SELECT firmware
    FROM device_readings
    WHERE temp_c <= 22
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM device_readings s
    INNER JOIN remaining_keys k ON s.firmware = k.firmware
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        CORR(pressure, temp_c) OVER (PARTITION BY device_code) AS corr_pressure_temp_c,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share,
        RANK() OVER (ORDER BY pressure NULLS FIRST) AS pressure_rank
    FROM verified
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        status_bits,
        firmware,
        corr_pressure_temp_c,
        humidity_share,
        pressure_rank,
        ROUND(humidity_share, 2) AS humidity_share_rounded
    FROM enriched
    WHERE reading_ts >= TIMESTAMP '2025-09-01 15:00:00'
      AND device_code IN ('DV-01', 'DV-04')
)
-- Grouped metrics
SELECT
    CASE WHEN status_bits >= 13 THEN 'HIGH' ELSE 'LOW' END AS status_bits_tier,
    BIT_OR(status_bits) AS status_bits_bit_or,
    STDDEV_SAMP(status_bits) AS status_bits_stddev
FROM cleaned
GROUP BY 1;

WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        pressure + humidity AS pressure_plus_humidity
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        new_group_flag,
        UPPER(device_code) AS device_code_upper
    FROM sessionized
    WHERE HOUR(reading_ts) BETWEEN 11 AND 15
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(status_bits, 1) AS status_bits_rounded,
        DATE_FORMAT(reading_ts, 'MMM yyyy') AS reading_ts_label
    FROM filtered
    WHERE reading_ts > TIMESTAMP '2025-09-01 11:00:00'
      AND pressure <= 994
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        device_code,
        COUNT_IF(device_code = 'DV-03') AS dv_03_events,
        SUM(CASE WHEN device_code = 'DV-02' THEN temp_c ELSE 0 END) AS dv_02_temp_c,
        APPROX_PERCENTILE(new_group_flag, ARRAY(0.25, 0.5, 0.75)) AS new_group_flag_quartiles,
        SIZE(COLLECT_SET(device_code)) AS num_device_codes
    FROM cleaned
    GROUP BY device_code
)
SELECT
    device_code,
    dv_02_temp_c,
    new_group_flag_quartiles,
    num_device_codes
FROM grouped
ORDER BY dv_02_temp_c DESC NULLS FIRST, num_device_codes

WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (ORDER BY temp_c DESC, id) AS temp_c_bucket,
        LAST_VALUE(temp_c) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_temp_c
    FROM device_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM with_windows
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c_bucket,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c_bucket
    FROM sessions_device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS row_num
    FROM deduped
    QUALIFY row_num = 1
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN temp_c_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS temp_c_bucket_tier,
        CASE WHEN humidity >= 65 THEN 'HIGH' ELSE 'LOW' END AS humidity_tier,
        SUM(temp_c_bucket) FILTER (WHERE device_code = 'DV-01') AS dv_01_total_temp_c_bucket,
        MIN(humidity) AS min_humidity,
        PERCENTILE_APPROX(status_bits, ARRAY(0.25, 0.5, 0.75)) AS status_bits_quartiles,
        CORR(humidity, status_bits) AS corr_humidity_status_bits,
        COUNT_IF(device_code = 'DV-03') AS dv_03_events,
        CORR(pressure, humidity) AS corr_pressure_humidity,
        GROUPING_ID() AS gid
    FROM latest
    GROUP BY GROUPING SETS ((CASE WHEN temp_c_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN humidity >= 65 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN temp_c_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END), ())
)
SELECT
    temp_c_bucket_tier,
    humidity_tier,
    dv_01_total_temp_c_bucket,
    min_humidity,
    corr_humidity_status_bits,
    dv_03_events,
    corr_pressure_humidity,
    gid
FROM daily
ORDER BY dv_01_total_temp_c_bucket NULLS LAST

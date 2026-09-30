WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        CASE WHEN device_code IN ('DV-02', 'DV-01') THEN 'Tier 1' ELSE 'Other' END device_code_group,
        pressure * temp_c pressure_times_temp_c
    FROM device_readings
    WHERE device_code != 'DV-01'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        temp_c,
        firmware,
        device_code_group,
        pressure_times_temp_c,
        MIN(humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_min_humidity,
        COUNT(humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_count_humidity,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_avg_humidity,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_sum_humidity,
        SUM(pressure_times_temp_c) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) pressure_times_temp_c_sum_2h
    FROM cleaned
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        firmware,
        device_code_group,
        pressure_times_temp_c,
        last4_min_humidity,
        last4_count_humidity,
        last4_avg_humidity,
        last4_sum_humidity,
        pressure_times_temp_c_sum_2h,
        CASE WHEN temp_c >= 37.9 THEN 'High' WHEN temp_c >= 21.7 THEN 'Medium' ELSE 'Low' END temp_c_band,
        CASE WHEN last4_min_humidity >= 77 THEN 'High' WHEN last4_min_humidity >= 46 THEN 'Medium' ELSE 'Low' END last4_min_humidity_tier
    FROM enriched
    WHERE pressure_times_temp_c_sum_2h >= 196
),
remaining_keys AS (
    -- Set difference on keys
    SELECT firmware, id
    FROM base
    EXCEPT DISTINCT
    SELECT firmware, id
    FROM device_readings
    WHERE device_code NOT IN ('DV-03', 'DV-02')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.firmware = k.firmware AND s.id = k.id
)
-- Grouped metrics
SELECT
    CASE WHEN last4_count_humidity >= 16 THEN 'LARGE' ELSE 'SMALL' END AS last4_count_humidity_tier,
    ROUND(AVG(last4_avg_humidity), 2) AS avg_last4_avg_humidity,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT firmware) AS distinct_firmwares
FROM verified
GROUP BY 1

WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(firmware, 1, 4) AS firmware_prefix,
        TRIM(device_code) AS clean_device_code
    FROM device_readings
    WHERE status_bits <= 5
),
filtered_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS(',', device_code, firmware) AS device_code_firmware_key
    FROM filtered
    WHERE reading_ts >= TO_DATE('2025-09-01')
      AND device_code NOT IN ('DV-04', 'DV-01', 'DV-03')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(firmware) AS firmware_lower
    FROM filtered_device_readings
    WHERE device_code IN ('DV-04', 'DV-03')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        clean_device_code,
        device_code_firmware_key,
        LAG(pressure) OVER w_recent AS prev_pressure,
        LEAD(pressure) OVER w_recent AS next_pressure,
        LAG(temp_c) OVER w_recent AS prev_temp_c,
        ROW_NUMBER() OVER w_recent AS row_seq,
        SUM(pressure) OVER (PARTITION BY device_code) AS device_code_sum_pressure
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY reading_ts, id)
)
-- Grouped metrics
SELECT
    device_code,
    CASE WHEN device_code_sum_pressure >= 3667 THEN 'PREMIUM' ELSE 'STANDARD' END AS device_code_sum_pressure_tier,
    MAX(prev_temp_c) FILTER (WHERE device_code = 'DV-02') AS dv_02_max_prev_temp_c,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(device_code_sum_pressure) FILTER (WHERE device_code = 'DV-04') AS dv_04_total_device_code_sum_pressure,
    ROUND(AVG(device_code_sum_pressure), 2) AS avg_device_code_sum_pressure
FROM enriched
GROUP BY CUBE (device_code, CASE WHEN device_code_sum_pressure >= 3667 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING avg_device_code_sum_pressure >= 0;

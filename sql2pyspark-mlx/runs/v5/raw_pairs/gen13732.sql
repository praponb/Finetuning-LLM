WITH matched_keys AS (
    -- Set difference on keys
    SELECT id, device_code
    FROM device_readings
    EXCEPT ALL
    SELECT id, device_code
    FROM device_readings
    WHERE HOUR(reading_ts) BETWEEN 10 AND 16
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM device_readings s
    INNER JOIN matched_keys k ON s.id = k.id AND s.device_code = k.device_code
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        ROUND(humidity * 0.85, 2) AS humidity_adjusted
    FROM reconciled
    WHERE reading_ts >= TIMESTAMP '2025-09-01 06:00:00'
      AND status_bits = 5
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS pressure_sum_30m,
        MAX(humidity_adjusted) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS humidity_adjusted_max_2h,
        COUNT(temp_c) OVER w_part AS temp_c_count_30m,
        CORR(pressure, status_bits) OVER w_grp AS corr_pressure_status_bits,
        COUNT(temp_c) OVER w_grp AS device_code_count_temp_c
    FROM base
    WHERE device_code IN ('DV-03', 'DV-02')
    WINDOW w_part AS (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY device_code)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(humidity_adjusted) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS humidity_adjusted_count_30m,
        ROUND(temp_c / SUM(temp_c) OVER (PARTITION BY device_code), 4) AS temp_c_share,
        RANK() OVER (PARTITION BY device_code ORDER BY humidity_adjusted_max_2h DESC NULLS LAST) AS humidity_adjusted_max_2h_rank,
        AVG(temp_c_count_30m) OVER w_recent AS temp_c_count_30m_avg_30m
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, temp_c - pressure_sum_30m AS temp_c_minus_pressure_sum_30m
    FROM metrics
    WHERE reading_ts >= TIMESTAMP '2025-09-01 08:00:00'
),
base_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        humidity_adjusted,
        pressure_sum_30m,
        humidity_adjusted_max_2h,
        temp_c_count_30m,
        corr_pressure_status_bits,
        device_code_count_temp_c,
        humidity_adjusted_count_30m,
        temp_c_share,
        temp_c_count_30m_avg_30m,
        temp_c_minus_pressure_sum_30m,
        CASE WHEN pressure >= 1028 THEN 'High' WHEN pressure >= 1003 THEN 'Medium' ELSE 'Low' END AS pressure_level,
        ROUND(pressure * 1.1, 2) AS pressure_adjusted
    FROM filtered
    WHERE temp_c_minus_pressure_sum_30m > 55
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_bits_avg_15m,
        COUNT(pressure_adjusted) OVER (PARTITION BY device_code) AS device_code_count_pressure_adjusted,
        MAX(humidity_adjusted) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS humidity_adjusted_max_30m
    FROM base_device_readings
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    pressure,
    status_bits,
    firmware,
    pressure_sum_30m,
    humidity_adjusted_max_2h
FROM enriched
ORDER BY reading_ts NULLS FIRST;

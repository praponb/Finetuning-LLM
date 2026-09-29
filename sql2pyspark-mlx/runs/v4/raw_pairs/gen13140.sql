WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        TRIM(device_code) AS clean_device_code
    FROM device_readings
    WHERE firmware IN ('1.0.3', '2.0.1', '1.1.0')
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN humidity >= 82 THEN 'High' WHEN humidity >= 41 THEN 'Medium' ELSE 'Low' END AS humidity_band
    FROM cleaned
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 07:00:00')
      AND humidity BETWEEN 59 AND 65
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM base
    INTERSECT ALL
    SELECT id
    FROM device_readings
    WHERE temp_c > 39
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN matched_keys k ON s.id = k.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(humidity) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS humidity_count_15m,
        COUNT(humidity) OVER w_hist AS humidity_count_1h,
        SUM(temp_c) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS temp_c_sum_2h,
        MAX(pressure) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS pressure_max_30m,
        MAX(temp_c) OVER w_hist AS temp_c_max_1h
    FROM reconciled
    WINDOW w_hist AS (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        CASE WHEN pressure_max_30m >= 1017 THEN 'PREMIUM' ELSE 'STANDARD' END AS pressure_max_30m_tier,
        PERCENTILE_APPROX(pressure_max_30m, ARRAY(0.25, 0.5, 0.75)) AS pressure_max_30m_quartiles,
        APPROX_PERCENTILE(humidity_count_15m, 0.5) AS humidity_count_15m_p50,
        ROUND(AVG(pressure), 2) AS avg_pressure,
        APPROX_PERCENTILE(pressure, 0.9) AS pressure_p90,
        APPROX_PERCENTILE(humidity, ARRAY(0.25, 0.5, 0.75)) AS humidity_quartiles,
        MIN(reading_ts) AS started_at,
        GROUPING_ID() AS grouping_id
    FROM metrics
    GROUP BY GROUPING SETS ((DATE_TRUNC('HOUR', reading_ts), CASE WHEN pressure_max_30m >= 1017 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('HOUR', reading_ts)), ())
    HAVING avg_pressure > 0
)
SELECT
    hour_window,
    avg_pressure,
    pressure_p90,
    humidity_quartiles,
    started_at,
    grouping_id
FROM summary;

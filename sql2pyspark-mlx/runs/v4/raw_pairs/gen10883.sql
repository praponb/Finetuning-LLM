WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        ROUND(status_bits * 1.07, 2) AS adjusted_status_bits
    FROM device_readings
    WHERE firmware NOT IN ('2.0.1', '1.0.3')
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(humidity * 0.9, 2) AS humidity_scaled,
        FLOOR(pressure) AS pressure_whole
    FROM filtered
    WHERE reading_ts >= TIMESTAMP '2025-09-01 13:00:00'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(humidity) OVER (ORDER BY adjusted_status_bits ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS humidity_avg_5_prior,
        NTILE(10) OVER (ORDER BY status_bits ASC, id ASC) AS status_bits_bucket
    FROM base
),
base_device_readings AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(humidity, 0) AS humidity_filled
    FROM scored
    WHERE status_bits_bucket != 5
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        adjusted_status_bits,
        humidity_scaled,
        pressure_whole,
        humidity_avg_5_prior,
        status_bits_bucket,
        humidity_filled,
        ROW_NUMBER() OVER w_hist AS row_seq,
        MAX(status_bits) OVER (ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_bits_max_30m
    FROM base_device_readings
    WINDOW w_hist AS (ORDER BY humidity_scaled DESC, id ASC)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics
    EXCEPT ALL
    SELECT id
    FROM device_readings
    WHERE HOUR(reading_ts) BETWEEN 11 AND 18
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.id = k.id
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
        CASE WHEN temp_c >= 38.6 THEN 'HIGH' ELSE 'LOW' END AS temp_c_tier,
        (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS duration_min,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(status_bits_max_30m, ARRAY(0.25, 0.5, 0.75)) AS status_bits_max_30m_quartiles,
        COUNT(DISTINCT id) AS distinct_ids_2
    FROM reconciled
    GROUP BY GROUPING SETS ((DATE_TRUNC('HOUR', reading_ts), CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN temp_c >= 38.6 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('HOUR', reading_ts), CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('HOUR', reading_ts)), ())
    HAVING COUNT(DISTINCT id) > 1
)
SELECT
    row_seq_tier,
    temp_c_tier,
    duration_min,
    distinct_ids,
    status_bits_max_30m_quartiles,
    distinct_ids_2
FROM summary;
